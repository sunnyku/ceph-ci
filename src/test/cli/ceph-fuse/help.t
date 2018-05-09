  $ ceph-fuse --help
  usage: ceph-fuse mountpoint [options]
  
  general options:
      -o opt,[opt...]        mount options
      -h   --help            print help
      -V   --version         print version
  
  FUSE options:
      -d   -o debug          enable debug output (implies -f)
      -f                     foreground operation
      -s                     disable multi-threaded operation
  
  usage: ceph-fuse [-n client.username] [-m mon-ip-addr:mon-port] <mount point> [OPTIONS]
    --client_mountpoint/-r <sub_directory>
                      use sub_directory as the mounted root, rather than the full Ceph tree.
  
    --conf/-c FILE    read configuration from the given configuration file
    --id ID           set ID portion of my name
    --name/-n TYPE.ID set name
    --cluster NAME    set cluster name (default: ceph)
    --setuser USER    set uid to user or uid (and gid to user's gid)
    --setgroup GROUP  set gid to group or gid
    --version         show version and quit
  
