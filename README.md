# Paketna obrada podataka - ASVSP

## Problem
Tema projektnog zadataka je analiza žalbi korisnika na usluge finansijskih kompanija 
na prostoru Sjedinjenih Američkih Država. Pročišćeni skup podataka (u nastavku skup podataka) dostupan je na 
[linku](https://drive.google.com/open?id=1giNPVFy6ACl3bvfj6hu1vR6GWGWXkQaI), dok je uvid u originalni skup podataka, 
koji se redovno ažurira, dostupan na [linku](https://www.consumerfinance.gov/data-research/consumer-complaints/search/?from=0&searchField=all&searchText=&size=25&sort=created_date_desc).

## Motivacija
Na svakodnevnom nivou pristiže veliki broj žalbi na usluge američkih finansijskih kompanija, pretežno banki.
Bankama je u interesu da zadrže što veći broj klijenata, te je neophodno da dosta pažnje posvete žalbama na usluge koje 
pružaju. Manuelna analiza velikog broja žalbi je mukotrpna, te bi bilo zgodno ukoliko bi se taj proces automatizovao.
Ovo se može uraditi upotrebom Apache Hadoop-a i njegovog MapReduce-a.

Pisanje MapReduce zadataka zna biti naporno, jer zahteva poznavanje nekog od programskih jezika 
koje Hadoop podržava, ali i poznavanje načina rada samog Hadoop-a. Zbog toga se u ovom projektu se 
analiza skupa podataka vrši upotrebom Apache Pig alata. U pitanju je alat koji koristi
posebno definisan Pig Latin jezik za iskazivanje analitičkih upita. Skripte pisane ovim jezikom se prevode na MapReduce
zadatke, koji se izvršavaju u okviru Hadoop-a. Razlog zbog koga sam se odlučio da koristim Apache Pig, a ne recimo Hive, 
jeste taj što je Pig Latin proceduralni jezik visokog nivoa. Kao i većina programera, prednost dajem jezicima 
koji se zasnivaju na konceptima proceduralnog programiranja, u odnosu na deklarativne jezika koji liče na SQL.
 Za detaljniji pregled Apache Pig alata može se pogledati sledeći [rad](https://drive.google.com/open?id=1--N_Dvrt1SCL5IKN83nTgv19ugZmDvbY),
 kao i [Apache Pig dokumentacija](https://pig.apache.org/).

## Analiza skupa podataka
Pokušao sam da pronađem odgovore na pitanja
- na koje kompanije se korisnici najviše žale ([complaints-per-company.pig](https://github.com/vladaindjic/paketna-obrada-ASVSP/blob/master/opisane-skripte/complaints-per-company.pig))
- na koje proizvode se korisnici najviše žale ([complaints-per-product.pig](https://github.com/vladaindjic/paketna-obrada-ASVSP/blob/master/opisane-skripte/complaints-per-product.pig))
- da li kompanije u propisanom roku odgovaraju na žalbe ([complaints-per-timelyResponse.pig](https://github.com/vladaindjic/paketna-obrada-ASVSP/blob/master/opisane-skripte/complaints-per-timelyResponse.pig))
- od svih žalbi jedne kompanije, koliki je procenat žalbi na kreditne kartice ([credit-card.pig](https://github.com/vladaindjic/paketna-obrada-ASVSP/blob/master/opisane-skripte/credit-card.pig))
- putem kojih sredstava za komunikaciju se žalbe najčešće podnoše ([complaints-per-submittedVia.pig](https://github.com/vladaindjic/paketna-obrada-ASVSP/blob/master/opisane-skripte/complaints-per-submittedVia.pig))
- koliko je prosečno vreme dostavljanja žalbe kompaniji u zavisnosti od sredstva podnošenja ([response-time.pig](https://github.com/vladaindjic/paketna-obrada-ASVSP/blob/master/opisane-skripte/response-time.pig))
- na koliki procenat žalbi kompanija odgovori sa zakašnjenjem ([late-response-percentage.pig](https://github.com/vladaindjic/paketna-obrada-ASVSP/blob/master/opisane-skripte/late-response-percentage.pig))
- koji vid komunikacije stariji Amerikanci najviš koriste za podnošenje žalbi ([older-people.pig](https://github.com/vladaindjic/paketna-obrada-ASVSP/blob/master/opisane-skripte/older-people.pig))

## Upotreba
U ovoj sekciju su date komande koje su neophodne da bi se neka od skripti pokrenula.
Ispred svake komande, odnosno grupe komandi, dat je komentar koji ukazuje na to da li 
je komandu/e potrebno pokrenuti na lokalnoj mašini (komenta počinje sa "local >") ili u docker
kontejneru (komentar počinje sa "resourcemanager >", pri čemu je resourcemanager ime kontejnera).

```bash
# local > kloniranje repozitorijuma (dovoljno jednom)
git clone https://github.com/vladaindjic/paketna-obrada-ASVSP.git
cd paketna-obrada-ASVSP

# local > podizanje docker kontejnera 
# Opcioni parametar --build se navodi ukoliko se želi naglasiti 
# da treba ponovno izgraditi sliku za resourcemanager kontejner
docker-compose up [--build] 

# local > kačenje na resourcemanager kontejner
docker exec -it resourcemanager /bin/bash

# resourcemanager > pravimo jedan direktorijum u koga ćemo kopirati skripte
mkdir complaints && cd complaints
# resourcemanager > na HDFS-u pravimo direktorijum za ulazne podatke
hdfs dfs -mkdir /input

# local > Kopiramo podatke i skriptu <naziv_skripte>.pig u resourcemanager kontejner.
# Skripte se nalaze u folderima paketna-obrada-ASVSP/opisane-skripte i
# paketna-obrada-ASVSP/ostale-skripte. 
# Neophodno je skinuti skup podataka sa linka https://drive.google.com/open?id=1giNPVFy6ACl3bvfj6hu1vR6GWGWXkQaI
# (u nastavku se podrazumeva da je skup podataka kopiran 
# u trenutni direktorijum pod nazivom complaints.csv)
docker cp complaints.csv resourcemanager:/complaints
docker cp <naziv_skripte>.pig resourcemanager:/complaints

# resourcemanager > kopiramo podatke na HDFS i pokrećemo skriptu <naziv_skripte.pig>
hdfs dfs -put complaints.csv /input
pig -x mapreduce <naziv_skripte>.pig

# resourcemanager > Prikazujemo rezultate na standardni izlaz i brisemo direktorijum
# sa izlaznim rezultatima (<output> predstavlja lokaciju na HDFS-u gde pokrenuta
# skripta smešta rezultate)
hdfs dfs -cat /<output>/*
hdfs dfs -rm -r -f /<output>

```

## Rezultati analize
Sažeti odgvori na prethodno postavljena pitanja dati su u komentarima skripti, a neki od njih i u ovom
[PDF dokumentu](https://github.com/vladaindjic/paketna-obrada-ASVSP/blob/master/asvsp-paketna-obrada.pdf).
Za detaljne odgovore, potrebno je pokrenuti željene skripte i očitati rezultate rada sa HDFS-a.