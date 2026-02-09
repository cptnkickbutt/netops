/queue type
add kind=pcq name=100_Download pcq-classifier=src-address,dst-address pcq-rate=100M
add kind=pcq name=125_Download pcq-classifier=src-address,dst-address pcq-rate=125M
add kind=pcq name=150_Download pcq-classifier=src-address,dst-address pcq-rate=150M
add kind=pcq name=250_Download pcq-classifier=src-address,dst-address pcq-rate=250M
add kind=pcq name=500_Download pcq-classifier=src-address,dst-address pcq-rate=500M
add kind=pcq name=1000_Download pcq-classifier=src-address,dst-address pcq-rate=1000M

/queue simple
add max-limit=250M/250M name=Internet queue=250_Download/250_Download target=Bridge_Internet,Bridge_Internet