# dataset-tracker

This is a Python script for pulling together many parameters about the resources on a CKAN portal and storing them in a local file. The script can be run regularly to inventory a data portal's datasets and resources.

When new datasets or new resources are found, a Slack notification can be sent. Removal of datasets or resources can also trigger Slack notifications.


Typical invocation:

> python track.py inventory False True

(where the "False" mutes notifications and the "True" puts the script into "quickmode" which suppresses attempts to determine the size of a table or other resource (which can be time-consuming)).
