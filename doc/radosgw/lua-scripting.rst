=============
Lua Scripting
=============

.. versionadded:: Pacific

.. contents::

This feature allows users to upload Lua scripts to different context in the radosgw. The two supported context are "preS3" that will execute a script before the 
S3 operation was taken, and "postS3" that will execute after each operation is taken. Script may be uploaded to address requests for users of a specific tenant.
The script can access fields in the request and modify some fields. All Lua language features can be used in the script.

.. toctree::
   :maxdepth: 1


Script Management via CLI
-------------------------

To upload a script:
   
::
   
   # radosgw-admin script put --infile={lua-file} --context={preS3|postS3} [--tenant={tenant-name}]


To print the content of the script to standard output:

::
   
   # radosgw-admin script get --context={preS3|postS3} [--tenant={tenant-name}]


To remove the script:

::
   
   # radosgw-admin script rm --context={preS3|postS3} [--tenant={tenant-name}]


Context Free Functions
----------------------
Debug Log
~~~~~~~~~
The ``RGWDebugLog()`` function accepts a string and prints it to the debug log with priority 20.
Each log message is prefixed ``Lua INFO:``. This function has no return value.

S3 Request Fields
-----------------

.. warning:: This feature is experimental. Fields may be removed or renamed in the future.

.. note::

    - Although Lua is a case-sensitive language, field names provided by the radosgw are case-insensitive. Function names remain case-sensitive.
    - Fields marked "optional" can have a nil value.
    - Fields marked as "iterable" can be used by the pairs() function and with the # length operator.
    - All table fields can be used with the bracket operator ``[]``.
    - ``time`` fields are strings with the following format: ``%Y-%m-%d %H:%M:%S``.


+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| Field                                                | Type     | Description                                                  | Iterable | Writeable | Optional |
+======================================================+==========+==============================================================+==========+===========+==========+
| ``S3Request.RGWOp``                                  | string   | radosgw operation                                            | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.DecodedURI``                             | string   | decoded URI                                                  | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.ContentLength``                          | integer  | size of the request                                          | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.GenericAttributes``                      | table    | string to string generic attributes map                      | yes      | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Response``                               | table    | response to the request                                      | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Response.HTTPStatusCode``                | integer  | HTTP status code                                             | no       | yes       | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Response.HTTPStatus``                    | string   | HTTP status text                                             | no       | yes       | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Response.RGWCode``                       | integer  | radosgw error code                                           | no       | yes       | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Response.Message``                       | string   | response message                                             | no       | yes       | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.SwiftAccountName``                       | string   | swift account name                                           | no       | no        | yes      |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Bucket``                                 | table    | info on the bucket                                           | no       | no        | yes      |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Bucket.Tenant``                          | string   | tenant of the bucket                                         | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Bucket.Name``                            | string   | bucket name                                                  | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Bucket.Marker``                          | string   | bucket marker (initial id)                                   | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Bucket.Id``                              | string   | bucket id                                                    | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Bucket.Count``                           | integer  | number of objects in the bucket                              | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Bucket.Size``                            | integer  | total size of objects in the bucket                          | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Bucket.ZoneGroupId``                     | string   | zone group of the bucket                                     | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Bucket.CreationTime``                    | time     | creation time of the bucket                                  | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Bucket.MTime``                           | time     | modification time of the bucket                              | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Bucket.Quota``                           | table    | bucket quota                                                 | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Bucket.Quota.MaxSize``                   | integer  | bucket quota max size                                        | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Bucket.Quota.MaxObjects``                | integer  | bucket quota max number of objects                           | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Bucket.Quota.Enabled``                   | boolean  | bucket quota is enabled                                      | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Bucket.Quota.Rounded``                   | boolean  | bucket quota is rounded to 4K                                | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Bucket.PlacementRule``                   | table    | bucket placement rule                                        | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Bucket.PlacementRule.Name``              | string   | bucket placement rule name                                   | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Bucket.PlacementRule.StorageClass``      | string   | bucket placement rule storage class                          | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Bucket.User``                            | table    | bucket owner                                                 | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Bucket.User.Tenant``                     | string   | bucket owner tenant                                          | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Bucket.User.Id``                         | string   | bucket owner id                                              | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Object``                                 | table    | info on the object                                           | no       | no        | yes      |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Object.Name``                            | string   | object name                                                  | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Object.Instance``                        | string   | object version                                               | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Object.Id``                              | string   | object id                                                    | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Object.Size``                            | integer  | object size                                                  | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Object.MTime``                           | time     | object mtime                                                 | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.CopyFrom``                               | table    | information on copy operation                                | no       | no        | yes      |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.CopyFrom.Tenant``                        | string   | tenant of the object copied from                             | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.CopyFrom.Bucket``                        | string   | bucket of the object copied from                             | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.CopyFrom.Object``                        | table    | object copied from. See: ``S3Request.Object``                | no       | no        | yes      |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.ObjectOwner``                            | table    | object owner                                                 | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.ObjectOwner.DisplayName``                | string   | object owner display name                                    | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.ObjectOwner.User``                       | table    | object user. See: ``S3Request.Bucket.User``                  | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.ZoneGroup.Name``                         | string   | name of zone group                                           | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.ZoneGroup.Endpoint``                     | string   | endpoint of zone group                                       | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.UserAcl``                                | table    | user ACL                                                     | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.UserAcl.Owner``                          | table    | user ACL owner. See: ``S3Request.Object.Owner``              | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.UserAcl.Grants``                         | table    | user ACL map of string to grant                              | yes      | no        | no       |
|                                                      |          | note: grants without an Id are not presented when iterated   |          |           |          |
|                                                      |          | and only one of them can be accessed via brackets            |          |           |          |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.UserAcl.Grants["<name>"]``               | table    | user ACL grant                                               | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.UserAcl.Grants["<name>"].Type``          | integer  | user ACL grant type                                          | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.UserAcl.Grants["<name>"].User``          | table    | user ACL grant user                                          | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.UserAcl.Grants["<name>"].User.Tenant``   | table    | user ACL grant user tenant                                   | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.UserAcl.Grants["<name>"].User.Id``       | table    | user ACL grant user id                                       | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.UserAcl.Grants["<name>"].GroupType``     | integer  | user ACL grant group type                                    | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.UserAcl.Grants["<name>"].Referer``       | string   | user ACL grant referer                                       | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.BucketAcl``                              | table    | bucket ACL. See: ``S3Request.UserAcl``                       | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.ObjectAcl``                              | table    | object ACL. See: ``S3Request.UserAcl``                       | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Environment``                            | table    | string to string environment map                             | yes      | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Policy``                                 | table    | policy                                                       | no       | no        | yes      |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Policy.Text``                            | string   | policy text                                                  | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Policy.Id``                              | string   | policy Id                                                    | no       | no        | yes      |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Policy.Statements``                      | table    | list of string statements                                    | yes      | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.UserPolicies``                           | table    | list of user policies                                        | yes      | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.UserPolicies[<index>]``                  | table    | user policy. See: ``S3Request.Policy``                       | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.RGWId``                                  | string   | radosgw host id: ``<host>-<zone>-<zonegroup>``               | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.HTTP``                                   | table    | HTTP header                                                  | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.HTTP.Parameters``                        | table    | string to string parameter map                               | yes      | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.HTTP.Resources``                         | table    | string to string resource map                                | yes      | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.HTTP.Metadata``                          | table    | string to string metadata map                                | yes      | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.HTTP.Host``                              | string   | host name                                                    | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.HTTP.Method``                            | string   | HTTP method                                                  | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.HTTP.URI``                               | string   | URI                                                          | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.HTTP.QueryString``                       | string   | HTTP query string                                            | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.HTTP.Domain``                            | string   | domain name                                                  | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Time``                                   | time     | request time                                                 | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Dialect``                                | string   | "S3" or "Swift"                                              | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Id``                                     | string   | request Id                                                   | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.TransactionId``                          | string   | transaction Id                                               | no       | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+
| ``S3Request.Tags``                                   | table    | object tags map                                              | yes      | no        | no       |
+------------------------------------------------------+----------+--------------------------------------------------------------+----------+-----------+----------+

S3 Request Functions
--------------------
Operations Log
~~~~~~~~~~~~~~
The ``S3Request.Log()`` function prints the requests into the operations log. This function has no parameters. It returns 0 for success and an error code if it fails.

Lua Code Samples
----------------
- Print information on source and destination objects in case of copy:

.. code-block:: lua
 
  function print_object(object)
    RGWDebugLog("  Name: " .. object.Name)
    RGWDebugLog("  Instance: " .. object.Instance)
    RGWDebugLog("  Id: " .. object.Id)
    RGWDebugLog("  Size: " .. object.Size)
    RGWDebugLog("  MTime: " .. object.MTime)
  end

  if S3Request.CopyFrom and S3Request.Object and S3Request.CopyFrom.Object then
    RGWDebugLog("copy from object:")
    print_object(S3Request.CopyFrom.Object)
    RGWDebugLog("to object:")
    print_object(S3Request.Object)
  end

- Print ACLs via a "generic function":

.. code-block:: lua

  function print_owner(owner)
    RGWDebugLog("Owner:")
    RGWDebugLog("  Dispaly Name: " .. owner.DisplayName)
    RGWDebugLog("  Id: " .. owner.User.Id)
    RGWDebugLog("  Tenanet: " .. owner.User.Tenant)
  end

  function print_acl(acl_type)
    index = acl_type .. "ACL"
    acl = S3Request[index]
    if acl then
      RGWDebugLog(acl_type .. "ACL Owner")
      print_owner(acl.Owner)
      RGWDebugLog("  there are " .. #acl.Grants .. " grant for owner")
      for k,v in pairs(acl.Grants) do
        RGWDebugLog("    Grant Key: " .. k)
        RGWDebugLog("    Grant Type: " .. v.Type)
        RGWDebugLog("    Grant Group Type: " .. v.GroupType)
        RGWDebugLog("    Grant Referer: " .. v.Referer)
        RGWDebugLog("    Grant User Tenant: " .. v.User.Tenant)
        RGWDebugLog("    Grant User Id: " .. v.User.Id)
      end
    else
      RGWDebugLog("no " .. acl_type .. " ACL in request: " .. S3Request.Id)
    end 
  end

  print_acl("User")
  print_acl("Bucket")
  print_acl("Object")

- Use of operations log only in case of errors:

.. code-block:: lua

  
  if S3Request.Response.HTTPStatusCode ~= 200 then
    RGWDebugLog("request is bad, use ops log")
    rc = S3Request.Log()
    RGWDebugLog("ops log return code: " .. rc)
  end

- Set values into the error message:

.. code-block:: lua

  if S3Request.Response.HTTPStatusCode == 500 then
    S3Request.Response.Message = "<Message> something bad happened :-( </Message>"
  end

