bq mk --table \
--schema "$3" \
--description "$1 results" \
--time_partitioning_field "$4" \
--time_partitioning_type "$5" \
gd-gcp-gridu-cloud-cert:results."$2"
