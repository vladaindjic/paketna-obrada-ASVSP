-- set CSV parser, beacuse of commas that are between quotes
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();

-- loading all complaints
allComplaints = LOAD '/home/complaints/complaints-valid.csv'
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
dateReceiveds = FOREACH allComplaints GENERATE dateReceived; 

-- group by product name
dateReceivedsGroup = GROUP dateReceiveds BY dateReceived;

-- count how many times each product name appers
dateReceivedsCount = FOREACH dateReceivedsGroup GENERATE group AS dateReceivedName:chararray, COUNT(dateReceiveds) AS count:long;

-- sort by number of appearance
dateReceivedsDesc = ORDER dateReceivedsCount BY count DESC;

first100 = LIMIT dateReceivedsDesc 100;

-- print on terminal
DUMP first100;
