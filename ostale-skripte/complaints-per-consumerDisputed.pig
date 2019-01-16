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
consumerDisputeds = FOREACH allComplaints GENERATE consumerDisputed; 

-- group by product name
consumerDisputedsGroup = GROUP consumerDisputeds BY consumerDisputed;

-- count how many times each product name appers
consumerDisputedsCount = FOREACH consumerDisputedsGroup GENERATE group AS consumerDisputedName:chararray, COUNT(consumerDisputeds) AS count:long;

-- sort by number of appearance
consumerDisputedsDesc = ORDER consumerDisputedsCount BY count DESC;
STORE consumerDisputedsDesc INTO 'hdfs://namenode:8020/output' using CSVExcelStorage(',');