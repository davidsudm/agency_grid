# Agency Grid application

Program that takes monthly data from the Agency Grid from the Agencies Department and
translates it into a language that the Finance Department needs to do the **Financial
Consolidation (FC)**.

Finance communicates us when a modification needs to be done for an entity, agency,
or department and is up to us to  add it into our mapping file called **Agency Grid
mapping** file in Excel (*.xlsx). It contains two sheets:

1. **Country mapping**:
It contains the FC code assigned to entities which are usually an agencies, but not always.
Usually an agency is linked to a country, but not always, hence the mapping.

2. **Department mapping**:
It contains the FC code assigned to the departments in an agency. In addition, It has some other
codes corresponding FTEs in training, long leave, service center, etc.