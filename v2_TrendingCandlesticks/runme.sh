#!/bin/bash

set -e

javac -cp java-api.jar:. com/lmax/gherkin/GherkinPeeler.java

java -cp java-api.jar:. com.lmax.gherkin.GherkinPeeler https://testapi.lmaxtrader.com ${LMAX_DEMO_USER} ${LMAX_DEMO_PASSWORD} CFD_DEMO | tee output.txt

