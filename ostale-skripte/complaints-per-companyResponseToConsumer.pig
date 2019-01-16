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
companyResponseToConsumers = FOREACH allComplaints GENERATE companyResponseToConsumer; 

-- group by product name
companyResponseToConsumersGroup = GROUP companyResponseToConsumers BY companyResponseToConsumer;

-- count how many times each product name appers
companyResponseToConsumersCount = FOREACH companyResponseToConsumersGroup GENERATE group AS companyResponseToConsumerName:chararray, COUNT(companyResponseToConsumers) AS count:long;

-- sort by number of appearance
companyResponseToConsumersDesc = ORDER companyResponseToConsumersCount BY count DESC;
STORE companyResponseToConsumersDesc INTO 'hdfs://namenode:8020/output' using CSVExcelStorage(',');