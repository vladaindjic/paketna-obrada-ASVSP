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

-- projection on property tag
submittedVias = FOREACH allComplaints GENERATE submittedVia; 

-- group by product name
submittedViasGroup = GROUP submittedVias BY submittedVia;

-- count how many times each product name appers
submittedViasCount = FOREACH submittedViasGroup GENERATE group AS submittedViaName:chararray, COUNT(submittedVias) AS count:long;

-- sort by number of appearance
submittedViasDesc = ORDER submittedViasCount BY count DESC;
STORE submittedViasDesc INTO 'hdfs://namenode:8020/output' using CSVExcelStorage(',');
