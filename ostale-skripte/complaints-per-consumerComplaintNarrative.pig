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
consumerComplaintNarratives = FOREACH allComplaints GENERATE consumerComplaintNarrative; 

-- group by product name
consumerComplaintNarrativesGroup = GROUP consumerComplaintNarratives BY consumerComplaintNarrative;

-- count how many times each product name appers
consumerComplaintNarrativesCount = FOREACH consumerComplaintNarrativesGroup GENERATE group AS consumerComplaintNarrativeName:chararray, COUNT(consumerComplaintNarratives) AS count:long;

-- sort by number of appearance
consumerComplaintNarrativesDesc = ORDER consumerComplaintNarrativesCount BY count DESC;
STORE consumerComplaintNarrativesDesc INTO 'hdfs://namenode:8020/output' using CSVExcelStorage(',');