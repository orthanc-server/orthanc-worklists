0.9.2 (2025-12-08)
==================

Fixes:
- Fix version number


0.9.0 (2025-11-18)
==================

Initial release.  This plugin is based on the Sample Modality worklist plugin that is provided in Orthanc repository.

- The Worklists plugin now provides a REST API to:
  - create worklists through `POST` at `/worklists/create`
  - list the worklists through `GET` at `/worklists`
  - view a single worklist through `GET` at `/worklists/{uuid}`
  - modify a worklist through `PUT` at `/worklists/{uuid}`
  - delete a worklist through `DELETE` at `/worklists/{uuid}`
  - samples are available in the [Orthanc book](https://orthanc.uclouvain.be/book/plugins/worklists-plugin-new.html)

- New configuration options:
  - `"SaveInOrthancDatabase"` to store the worklists in the Orthanc DB (provided that you are using SQLite or PostgreSQL).
  - `"DeleteWorklistsOnStableStudy"` to delete the worklist once its related study has been received and is stable.
  - `"SetStudyInstanceUidIfMissing"` to add a StudyInstanceUID if the REST API request does not include one.
  - `"DeleteWorklistsDelay"` to delete a worklist N hours after it has been created
    (only available if using the "SaveInOrthancDatabase" mode).
  - `"HousekeepingInterval"` to define the delay between 2 executions of the Worklist Housekeeping thread
    that deletes the worklists when required.

- **Note:** the previous `"Database"` configuration has now been renamed in `"Directory"` to better differentiate the `File` or `DB` modes.

