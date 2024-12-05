#!/bin/bash

#    Copyright (C) 2024-present Codein, Inc.
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the Server Side Public License, version 1,
#    as published by MongoDB, Inc.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    Server Side Public License for more details.
#
#    You should have received a copy of the Server Side Public License
#    along with this program. If not, see
#    <http://www.mongodb.com/licensing/server-side-public-license>.
#
#    As a special exception, the copyright holders give permission to link the
#    code of portions of this program with the OpenSSL library under certain
#    conditions as described in each individual source file and distribute
#    linked combinations including the program with the OpenSSL library. You
#    must comply with the Server Side Public License in all respects for
#    all of the code used other than as permitted herein. If you modify file(s)
#    with this exception, you may extend this exception to your version of the
#    file(s), but you are not obligated to do so. If you do not wish to do so,
#    delete this exception statement from your version. If you delete this
#    exception statement from all source files in the program, then also delete
#    it in the license file.
#

# customers-2000000.zip
if ! test -f /tmp/customers-2000000.zip; then
    echo -n "Downloading customers-2000000.zip..."
    wget -q -o /tmp/customers-2000000-wget.log -O /tmp/customers-2000000.zip "https://drive.usercontent.google.com/download?id=1IXQDp8Um3d-o7ysZLxkDyuvFj9gtlxqz&export=download&authuser=0
https://drive.usercontent.google.com/download?id=1IXQDp8Um3d-o7ysZLxkDyuvFj9gtlxqz&export=download&authuser=0&confirm=t&uuid=a43ba902-7c1b-43b6-9462-338fc925f45b&at=APZUnTXxsPTlhTgKImAozx7cRQ_L%3A1715287384041" -N
    echo "done"
    echo ""
fi

# people-2000000.zip
if ! test -f /tmp/people-2000000.zip; then
    echo -n "Downloading people-2000000.zip..."
    wget -q -o /tmp/people-2000000-wget.log -O /tmp/people-2000000.zip "https://drive.usercontent.google.com/download?id=1fveqbEJIr4o4oMqswF03NA2Qrk1zF7v4&export=download&authuser=0
https://drive.usercontent.google.com/download?id=1fveqbEJIr4o4oMqswF03NA2Qrk1zF7v4&export=download&authuser=0&confirm=t&uuid=d901b7b8-d4e0-4251-8ca6-5129c69b9584&at=APZUnTXEIyvdrHoA0H6izUHxtebp%3A1715287392221" -N
    echo "done"
    echo ""
fi

# organizations-2000000.zip
if ! test -f /tmp/organizations-2000000.zip; then
    echo -n "Downloading organizations-2000000.zip..."
    wget -q -o /tmp/organizations-2000000-wget.log -O /tmp/organizations-2000000.zip "https://drive.usercontent.google.com/download?id=18vlOi20KcMR328ewc2NBsoBNPrV3vL9Q&export=download&authuser=0
https://drive.usercontent.google.com/download?id=18vlOi20KcMR328ewc2NBsoBNPrV3vL9Q&export=download&authuser=0&confirm=t&uuid=a1c3fe85-c137-41cc-a1ca-37509ec8f19b&at=APZUnTWFb_Xo8jBEs2SoKBKt5qRP%3A1715286470979" -N
    echo "done"
    echo ""
fi

if ! test -f /tmp/customers-2000000.csv; then
    echo -n "Preparing customers-2000000.csv..."
    7z x /tmp/customers-2000000.zip -so -y | tail -n +2 > /tmp/customers-2000000.csv
    echo "Index/int32,Customer Id/string,First Name/string,Last Name/string,Company/string,City/string,Country/string,Phone 1/string,Phone 2/string,Email/string,Subscription Date/string,Website/string" > /tmp/customers.txt
    echo "done"
    echo ""
fi

if ! test -f /tmp/people-2000000.csv; then
    echo -n "Preparing people-2000000.csv..."
    7z x /tmp/people-2000000.zip -so -y | tail -n +2 > /tmp/people-2000000.csv
    echo 'Index/int32,User Id/string,First Name/string,Last Name/string,Sex/string,Email/string,Phone/string,Date of birth/string,Job Title/string' > /tmp/people.txt
    echo "done"
    echo ""
fi

if ! test -f /tmp/organizations-2000000.csv; then
    echo -n "Preparing organizations-2000000.csv..."
    7z x /tmp/organizations-2000000.zip -so -y | tail -n +2 > /tmp/organizations-2000000.csv
    echo 'Index/int32,Organization Id/string,Name/string,Website/string,Country/string,Description/string,Founded/string,Industry/string,Number of employees/int32' > /tmp/organizations.txt
    echo "done"
    echo ""
fi
