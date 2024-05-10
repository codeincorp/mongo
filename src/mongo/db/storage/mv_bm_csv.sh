#!/bin/sh

# customers-2000000
if ! test -f /tmp/customers-2000000.zip; then
    wget -O /tmp/customers-2000000.zip "https://drive.usercontent.google.com/download?id=1IXQDp8Um3d-o7ysZLxkDyuvFj9gtlxqz&export=download&authuser=0
https://drive.usercontent.google.com/download?id=1IXQDp8Um3d-o7ysZLxkDyuvFj9gtlxqz&export=download&authuser=0&confirm=t&uuid=a43ba902-7c1b-43b6-9462-338fc925f45b&at=APZUnTXxsPTlhTgKImAozx7cRQ_L%3A1715287384041" -N
fi

# people-2000000
if ! test -f /tmp/people-2000000.zip; then
    wget -O /tmp/people-2000000.zip "https://drive.usercontent.google.com/download?id=1fveqbEJIr4o4oMqswF03NA2Qrk1zF7v4&export=download&authuser=0
https://drive.usercontent.google.com/download?id=1fveqbEJIr4o4oMqswF03NA2Qrk1zF7v4&export=download&authuser=0&confirm=t&uuid=d901b7b8-d4e0-4251-8ca6-5129c69b9584&at=APZUnTXEIyvdrHoA0H6izUHxtebp%3A1715287392221" -N
fi

# organizations-2000000
if ! test -f /tmp/organizations-2000000.zip; then
    wget -O /tmp/organizations-2000000.zip "https://drive.usercontent.google.com/download?id=18vlOi20KcMR328ewc2NBsoBNPrV3vL9Q&export=download&authuser=0
https://drive.usercontent.google.com/download?id=18vlOi20KcMR328ewc2NBsoBNPrV3vL9Q&export=download&authuser=0&confirm=t&uuid=a1c3fe85-c137-41cc-a1ca-37509ec8f19b&at=APZUnTWFb_Xo8jBEs2SoKBKt5qRP%3A1715286470979" -N
fi

if ! test -f /tmp/customers-2000000.csv; then
    tar -xzvf /tmp/customers-2000000.zip -C /tmp/
    echo "Index/int32,Customer Id/string,First Name/string,Last Name/string,Company/string,City/string,Country/string,Phone 1/string,Phone 2/string,Email/string,Subscription Date/string,Website/string" > /tmp/customers.txt
fi

if ! test -f /tmp/people-2000000.csv; then
    tar -xzvf /tmp/people-2000000.zip -C /tmp/
    echo 'Index/int32,User Id/string,First Name/string,Last Name/string,Sex/string,Email/string,Phone/string,Date of birth/string,Job Title/string' > /tmp/people.txt

fi
 
if ! test -f /tmp/organizations-2000000.csv; then
    tar -xzvf /tmp/organizations-2000000.zip -C /tmp/
    echo 'Index/int32,Organization Id/string,Name/string,Website/string,Country/string,Description/string,Founded/string,Industry/string,Number of employees/int32' > /tmp/organizations.txt
fi
