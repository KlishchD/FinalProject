gcloud kms keyrings create "aggregations" --location "europe-central2"
gcloud kms keys create "results" --location "europe-central2" --purpose "encryption" --keyring "aggregations"