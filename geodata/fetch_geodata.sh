#!/bin/bash


COUNTRY_BLOCKS_FILE=GeoLite2-Country-Blocks-IPv4.csv
COUNTRY_LOCATIONS_FILE=GeoLite2-Country-Locations.csv
COUNTRY_LOCATIONS_EN_FILE=GeoLite2-Country-Locations-en.csv
ARCHIVE_NAME=GeoLite2-Country-CSV

if [[ -f ${COUNTRY_BLOCKS_FILE} ]] & [[ -f ${COUNTRY_LOCATIONS_FILE} ]]
  then
    echo "GeoLite2 Files already exist."
    exit 0
fi

rm -rf ${ARCHIVE_NAME}_*
FILE=${ARCHIVE_NAME}.zip
wget http://geolite.maxmind.com/download/geoip/database/${FILE}
unzip -o ${FILE}
rm -f ${FILE}
mv ${ARCHIVE_NAME}_*/${COUNTRY_BLOCKS_FILE} ${COUNTRY_BLOCKS_FILE}
mv ${ARCHIVE_NAME}_*/${COUNTRY_LOCATIONS_EN_FILE} ${COUNTRY_LOCATIONS_FILE}
rm -rf ${ARCHIVE_NAME}_*