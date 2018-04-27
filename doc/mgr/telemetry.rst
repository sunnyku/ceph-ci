Telemetry plugin
================
The telemetry plugin sends anonymous data about the cluster is it running in back to the Ceph project.

The data being send back to the project does not contain any sensitive data like pool names, object names, object contents or hostnames.

It contains counters and statistics on how the cluster has been deployed, the version of Ceph, the distribition it runs on and other parameters which help the project to gain a better understanding of the way Ceph is used.

Data is send over HTTPS to *telemetry.ceph.com*

Enabling
--------

The *telemetry* module is enabled with::

  ceph mgr module enable telemetry


Interval
--------
The module compiles and sends a new report every 72 hours.

Contact and Description
-----------------------
A contact and description can be added to the report, this is optional.

  ceph telemetry config-set contact 'John Doe <john.doe@example.com>'
  ceph telemetry config-set description 'My first Ceph cluster'

Show report
-----------
The report being send back is in JSON format and can be requested from the module:

  ceph telemetry show

The JSON data which sould be send is now returned and can be inspected.
