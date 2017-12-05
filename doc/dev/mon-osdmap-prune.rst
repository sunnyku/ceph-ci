===========================
FULL OSDMAP VERSION PRUNING
===========================

For each incremental osdmap epoch, the monitor will keep a full osdmap
epoch in the store.

While this is great when serving osdmap requests from clients, allowing
us to fulfill their request without having to recompute the full osdmap
from a myriad of incrementals, it can also become a burden once we start
keeping an unbounded number of osdmaps.

The monitors will attempt to keep a bounded number of osdmaps in the store.
This number is defined (and configurable) via ``mon_min_osdmap_epochs``, and
defaults to 500 epochs. Generally speaking, we will remove older osdmap
epochs once we go over this limit.

However, there are a few constraints to removing osdmaps. These are all
defined in ``OSDMonitor::get_trim_to()``.

In the event one of these conditions is not met, we may go over the bounds
defined by ``mon_min_osdmap_epochs``. And if the cluster does not meet the
trim criteria for some time (e.g., unclean pgs), the monitor may start
keeping a lot of osdmaps. This can start putting pressure on the underlying
key/value store, as well as on the available disk space.

One way to mitigate this problem would be to stop keeping full osdmap
epochs on disk. We would have to rebuild osdmaps on-demand, or grab them
from cache if they had been recently served. We would still have to keep
at least one osdmap, and apply all incrementals on top of either this
oldest map epoch kept in the store or a more recent map grabbed from cache.
While this would be feasible, it seems like a lot of cpu (and potentially
IO) would be going into rebuilding osdmaps.

Additionally, this would prevent the aforementioned problem going forward,
but would do nothing for stores currently in a state that would truly
benefit from not keeping osdmaps.

This brings us to full osdmap pruning.

Instead of not keeping full osdmap epochs, we are going to prune some of
them when we have too many.

Deciding whether we have too many will be dictated by a configurable option
``mon_osdmap_full_prune_min`` (default: 10000). The pruning algorithm will be
engaged once we go over this threshold.

We will not remove all ``mon_osdmap_full_prune_min`` full osdmap epochs
though. Instead, we are going to poke some holes in the sequence of full
maps. By default, we will keep one full osdmap per 10 maps since the last
map kept; i.e., if we keep epoch 1, we will also keep epoch 10 and remove
full map epochs 2 to 9. The size of this interval is configurable with
``mon_osdmap_full_prune_interval``.

Essentially, we are proposing to keep ~10% of the full maps, but we will
always honour the minimum number of osdmap epochs, as defined by
``mon_min_osdmap_epochs``, and these won't be used for the count of the
minimum versions to prune. For instance, if we have on-disk versions
[1..50000], we would allow the pruning algorithm to operate only over
osdmap epochs [1..49500); but, if have on-disk versions [1..10200], we
won't be pruning because the algorithm would only operate on versions
[1..700), and this interval contains less versions than the minimum
required by ``mon_osdmap_full_prune_min``.


ALGORITHM
=========

Say we have 50,000 osdmap epochs in the store, and we're using the
defaults for all configurable options.

::

    -----------------------------------------------------------
    |1|2|..|10|11|..|100|..|1000|..|10000|10001|..|49999|50000|
    -----------------------------------------------------------
     ^ first                                            last ^

We will prune when all the following constraints are met:

1. number of versions is greater than ``mon_min_osdmap_epochs``

2. the number of versions between 'first' and 'prune_to' is greater than
``mon_osdmap_full_prune_min``, with 'prune_to' being equal to 'last'
minus ``mon_min_osdmap_epochs``.

If any of these conditions fails, we will *not* prune any maps.

Furthermore, if it is known that we have been pruning, but since then we
are no longer satisfying at least one of the above constraints, we will
not continue to prune. In essence, we only prune full osdmaps if the
number of epochs in the store so warrants it.

As pruning will create gaps in the sequence of full maps, we need to
first decide which maps will be pinned for the pruning process; i.e.,
which maps will not be removed, potentially being used in the future as
the last known full state when rebuilding an osdmap. All pinned maps will
be kept in a set (thus ensuring we get ordered, unique epochs).

Deciding which maps to pin is straightforward and happens before pruning.::

    first_to_pin = first
    last_to_pin = last - mon_min_osdmap_epochs

    for e in [first_to_pin .. last_to_pin]:
      if pinned.empty() OR
         e - pinned.last() == mon_osdmap_full_prune_interval:
        pinned.insert(e)

    pinned.insert(last_to_pin)

The simplicity of the pinning process has the obvious drawback of not
splitting the available versions uniformly. While we ensure that most
pinned maps are ``mon_osdmap_full_prune_interval`` epochs apart, the last
two pinned maps may very well be sequential. We are taking no further
steps to resolve this particular corner case because we don't think it
will nefariously affect the pruning process.

With our maps pinned, we will now be able to remove the maps in-between.

We could do the removal in one go, but we have no idea how long that would
take. Therefore, we will perform several iterations, removing at most
``mon_osdmap_full_prune_txsize`` osdmaps per iteration.

In the end, our on-disk map sequence will look similar to::

    ------------------------------------------
    |1|10|20|30|..|49500|49501|..|49999|50000|
    ------------------------------------------
     ^ first                           last ^


Because we are not pruning all versions in one go, we need to keep state
about how far along on our pruning we are. With that in mind, we have
created a data structure, ``osdmap_manifest_t``, that holds the set of pinned
maps and the last pruned epoch: ::

    struct osdmap_manifest_t:
        set<version_t> pinned;
        version_t      last_pruned;

The osdmap manifest will be written to disk the first time we prune maps.
We ensure that the manifest is written to disk with every transaction
resulting from a pruning round, so that we always have an up-to-date
manifest.

We also rely on the manifest existing on disk to ascertain whether we are in
the middle of pruning, or have pruned in the past. If we have pruned at
least once, we will require the manifest so that we know which maps have
been pinned, in order to rebuild osdmaps from incremental maps.
Additionally, we need to know which maps we have pinned during osdmap
trimming, but we will discuss that later; for now, let us focus on the
prunning algorithm.

For simplicity, we have decided that we shall not further along the pruning
interval if a pruning is yet to be finished.

Upon starting a full osdmap prune, we will pin all the maps we need (as
discussed before), and will keep the pinned map vector in the osdmap manifest.
The manifest will also be initiated with ``last_pruned`` at zero, denoting
that there hasn't been a single version over ``first_committed`` that has been
removed; in reality, these should be the criteria when ascertaining whether
pruning has been started, not started, or has finished:

* We will have not started pruning if ::

    invariant: first_committed > 1
 
    last_pruned < first_committed

* We will have started pruning if ::

    invariant: first_committed > 0
    invariant: !pinned.empty())
    invariant: pinned.count(first_committed) == 1
    invariant: last_pruned > 0
    invariant: last_pruned < last_committed

    precond: last_pruned > first_committed AND
             last_pruned < pinned.last() AND
             !finished_pruning

    postcond: last_pruned > first_committed AND
              last_pruned < pinned.last() AND
              upper_pinned(last_pruned) C pinned AND
              lower_pinned(last_pruned) C pinned AND
              upper_pinned(last_pruned) != lower_pinned(last_pruned)

* We will have finished pruning if ::

    invariant: first_committed > 0
    invariant: !pinned.empty()
    invariant: last_pruned > 0
    invariant: last_pruned < last_committed

    (pinned.count(last_pruned + 1) AND (
        last_pruned + 1 == pinned.last() OR
        for each m1,m2 in pinned[last_pinned+1..pinned.last()]: m2 == m1+1
    )) OR
    last_pruned < first_committed

Once we finish pruning maps, we will keep the manifest in the store, to
allow us to easily find which maps have been pinned (instead of checking
the store until we find a map), and to tell us which epoch did we last
prune in case we need to prune again in the future. This doesn't however
mean we will forever keep the osdmap manifest: the osdmap manifest will
no longer be required once the monitor trims osdmaps and earliest available
epoch in the store is greater than the last map we pruned.

Pruning further maps may be cancelled at any point if the monitor trims
osdmap epochs. The same conditions from ``OSDMonitor::get_trim_to()`` that
force the monitor to keep a lot of osdmaps, thus requiring us to prune, may
eventually change and allow the monitor to remove some of its oldest maps.

If the monitor trims maps, we must then adjust the osdmap manifest to
reflect our pruning status, or remove the manifest entirely if it no longer
makes sense to keep it. For instance, take the map sequence from before, but
let us assume we did not finish pruning all the maps. ::

    -------------------------------------------------------------
    |1|10|20|30|..|490|500|501|502|..|49500|49501|..|49999|50000|
    -------------------------------------------------------------
     ^ first          ^ last_pruned                       last ^
                          (e 499)

    pinned = {1, 10, 20, ..., 490, 500, 510, ..., 49500}

Now let us assume that the monitor will trim up to epoch 501. This means
removing all maps prior to epoch 501, and updating the ``first_committed``
pointer to ``501``. Given removing all those maps would invalidate our
current pruning process, we can consider our pruning has finished and drop
our osdmap manifest. Doing so also simplifies starting a new prune, if all
the starting conditions are met once we refreshed our state from the
store.

We would then have the following map sequence: ::

    ---------------------------------------
    |501|502|..|49500|49501|..|49999|50000|
    ---------------------------------------
     ^ first                        last ^

However, imagine a slightly more convoluted scenario: the monitor will trim
up to epoch 491. In this case, epoch 491 has been removed from the store.

Given we will always need to have the oldest known map in the store, before
we trim we will have to check whether that map is in the prune interval
(i.e., if it's lower or equal than ``last_pruned``). If so, we need to check
if this is a pinned map, in which case we don't have much to be concerned
aside from removing lower epochs from the manifest's pinned set. On the
other hand, if the map being trimmed to is not a pinned map, we will need
to rebuild the map and pin it and remove the pinned maps prior to the this
epoch. 

In this case, we would end up with the following sequence:::

    -----------------------------------------------
    |491|500|501|502|..|49500|49501|..|49999|50000|
    -----------------------------------------------
     ^  ^- last_prunned (e 499)             last ^
     `- first

There is still an edge case that we should mention. Consider that we are
going to trim up to epoch 499, which is the very last pruned epoch.

Much like the scenario above, we would end up writing osdmap epoch 499 to
the store; but what should we do about pinned maps and pruning?

The simplest solution is to drop the osdmap manifest. After all, given we
are trimming to the last pruned map, and we are rebuilding this map, we can
guarantee that all maps greater than e 499 are sequential (because we have
not pruned any of them). In essence, dropping the osdmap manifest in this
case is essentially the same as if we were trimming over the last pruned
epoch: we can prune again later if we meet the required conditions.

Once we finish trimming maps, the following constraints need to be observed:::

    if last_pruned <= first_committed:
        postcond: !store.exists(osdmap_manifest)
        postcond: store.exists_full(first_committed)
        postcond: for v1,v2 in ondisk_full_osdmaps: v2 == v1 + 1

    else:
        postcond: last_pruned > first_committed
        postcond: store.exists(osdmap_manifest)
        postcond: manifest.pinned(first_committed)
        postcond: manifest.pinned.first() == first_committed
        postcond: store.exists_full(first_committed)

And, with this, we have fully dwelled into full osdmap pruning. Enjoy.

