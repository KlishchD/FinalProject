bq mk --table \
--description "$1 results" \
gd-gcp-gridu-cloud-cert:results."$2" \
"$3"