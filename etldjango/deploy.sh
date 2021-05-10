# Setup principal variables
gcloud auth list
gcloud config list project
_PROJECT_ID=$(gcloud config get-value core/project)
REGION=us-central1

# Create Bucket
#GS_BUCKET_NAME=${_PROJECT_ID}-media
#gsutil mb -l ${REGION} gs://${GS_BUCKET_NAME}

# Create .env secrets by secretmanager
gcloud secrets create application_settings_etl --data-file .env
export PROJECTNUM=$(gcloud projects describe ${_PROJECT_ID} --format 'value(projectNumber)')
export CLOUDRUN=${PROJECTNUM}-compute@developer.gserviceaccount.com
rm .env

gcloud secrets add-iam-policy-binding application_settings_etl \
  --member serviceAccount:${CLOUDRUN} --role roles/secretmanager.secretAccessor

gcloud secrets versions list application_settings_etl

# Allow access to components
export PROJECTNUM=$(gcloud projects describe ${_PROJECT_ID} --format 'value(projectNumber)')
export CLOUDBUILD=${PROJECTNUM}@cloudbuild.gserviceaccount.com

# Build  Docker container
gcloud builds submit --tag gcr.io/$_PROJECT_ID/opencovid2-etl
gcloud container images list

# Past secrets to the Docket container
gcloud secrets add-iam-policy-binding application_settings_etl \
  --member serviceAccount:${CLOUDBUILD} --role roles/secretmanager.secretAccessor

gcloud projects add-iam-policy-binding ${_PROJECT_ID} \
    --member serviceAccount:${CLOUDBUILD} --role roles/cloudsql.client

# Run Migrations
gcloud builds submit --config cloudmigrate.yaml 

# Django on Cloud Run
gcloud beta run deploy opencovid2-etl --platform managed --region $REGION \
  --cpu 2 \
  --timeout 3600 \
  --memory 4096 \
  --image gcr.io/$_PROJECT_ID/opencovid2-etl \
  --add-cloudsql-instances ${_PROJECT_ID}:${REGION}:opencovid-db \
  --allow-unauthenticated