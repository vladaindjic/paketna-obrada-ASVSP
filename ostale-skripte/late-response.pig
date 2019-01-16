/*
Koje firme kasno odgovaraju na zahteve korisnika. Da li mozda one imaju i najveci broj zalbi?
*/

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

-- projection on propery company
lateComplaints = FILTER allComplaints BY timelyResponse == 'No'; 

lateCompanies = FOREACH lateComplaints GENERATE company, timelyResponse;
-- group by company name
lateCompaniesGroup = GROUP lateCompanies BY company;

-- count how many times each company name appers
lateCompaniesCount = FOREACH lateCompaniesGroup GENERATE group AS companyName:chararray, COUNT(lateCompanies.company) AS count:long;

-- sort by number of appearance
sortLateCompaniesDesc = ORDER lateCompaniesCount BY count DESC;

-- print on terminal
DUMP sortLateCompaniesDesc;


/*

Output:

(WELLS FARGO & COMPANY,3136) 		     67422 (5.)
(BANK OF AMERICA, NATIONAL ASSOCIATION,1583) 79440 (4.)
(EQUIFAX, INC.,1542) 			     100580 (1.)
(OCWEN LOAN SERVICING LLC,542)		     27231 (9.)
(CITIBANK, N.A.,359)			     45765 (7.)
(Colony Brands, Inc.,347)
(Mobiloans, LLC,347)
(Southwest Credit Systems, L.P.,304)
(Midwest Recovery Systems,253)
(Residential Credit Solutions, Inc.,173)


*/


-- get first 100
-- first100 = LIMIT sortCompaniesDesc 100;

-- store
-- STORE first100 INTO '/home/complaints/generated/complaints-per-company' USING CSVExcelStorage(',');
