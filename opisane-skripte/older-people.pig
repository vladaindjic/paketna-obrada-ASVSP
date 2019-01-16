/*
Sta stariji Amerikanci najcesce koriste kao sredstvo za javljanje.
*/


-- set CSV parser, beacuse of commas that are between quotes
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();

-- loading all complaints
allComplaints = LOAD 'hdfs://namenode:8020/input/complaints.csv'
   USING CSVExcelStorage(',')
   as ( dateReceived: chararray,
        product: chararray,
        subProduct: chararray,
        issue: chararray,
        subIssue: chararray,
        consumerComplaintNarrative: chararray,
        companyPublicResponse: chararray,
        company: chararray,
        state: chararray,
        zipCode: chararray,
        tags: chararray,
        consumerConsentProvided: chararray,
        submittedVia: chararray,
        dateSentToCompany: chararray,
        companyResponseToConsumer: chararray,
        timelyResponse: chararray,
        consumerDisputed: chararray,
        complaintId: chararray);

olderPeopleComplaints = FILTER allComplaints by (tags matches '.*[Oo]lder.*');
olderPeopleGroupBySubmittedVia = GROUP olderPeopleComplaints by submittedVia;
olderPeoplePerSubmittedVia = FOREACH olderPeopleGroupBySubmittedVia generate group, COUNT(olderPeopleComplaints.tags) as (countPerSubmittedVia:long);
orderDescOlderPeoplePerSubmittedVia = ORDER olderPeoplePerSubmittedVia BY countPerSubmittedVia DESC;
-- dump orderDescOlderPeoplePerSubmittedVia;

STORE orderDescOlderPeoplePerSubmittedVia INTO 'hdfs://namenode:8020/output' using CSVExcelStorage(',');

/*
    Output:
    (Web,61376)
    (Phone,13073)
    (Postal mail,5156)
    (Referral,4474)
    (Fax,1209)
    (Email,6)

*/

