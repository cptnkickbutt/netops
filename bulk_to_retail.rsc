/queue type
add kind=pcq name=50_Download pcq-classifier=src-address,dst-address pcq-rate=50M
add kind=pcq name=100_Download pcq-classifier=src-address,dst-address pcq-rate=100M
add kind=pcq name=125_Download pcq-classifier=src-address,dst-address pcq-rate=125M
add kind=pcq name=150_Download pcq-classifier=src-address,dst-address pcq-rate=150M
add kind=pcq name=250_Download pcq-classifier=src-address,dst-address pcq-rate=250M
/queue simple
add max-limit=150M/150M name=Internet queue=150_Download/150_Download target=Bridge_Internet,Bridge_Internet

