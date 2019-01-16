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
zipCodes = FOREACH allComplaints GENERATE zipCode; 

-- group by product name
zipCodesGroup = GROUP zipCodes BY zipCode;

-- count how many times each product name appers
zipCodesCount = FOREACH zipCodesGroup GENERATE group AS zipCodeName:chararray, COUNT(zipCodes) AS count:long;

-- sort by number of appearance
sortZipCodesDesc = ORDER zipCodesCount BY count DESC;
STORE sortZipCodesDesc INTO 'hdfs://namenode:8020/output' using CSVExcelStorage(',');