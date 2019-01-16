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

-- projection on property product
subIssues = FOREACH allComplaints GENERATE subIssue; 

-- group by product name
subIssuesGroup = GROUP subIssues BY subIssue;

-- count how many times each product name appers
subIssuesCount = FOREACH subIssuesGroup GENERATE group AS subIssueName:chararray, COUNT(subIssues) AS count:long;

-- sort by number of appearance
sortSubIssuesDesc = ORDER subIssuesCount BY count DESC;
STORE sortSubIssuesDesc INTO 'hdfs://namenode:8020/output' using CSVExcelStorage(',');