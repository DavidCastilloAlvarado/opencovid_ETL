# Setup principal variables
gcloud auth list
gcloud config list project
PROJECT_ID=$(gcloud config get-value core/project)
REGION=us-central1

# Create Bucket
#GS_BUCKET_NAME=${PROJECT_ID}-media
#gsutil mb -l ${REGION} gs://${GS_BUCKET_NAME}

# Create .env secrets by secretmanager
gcloud secrets create application_settings_etl --data-file .env
export PROJECTNUM=$(gcloud projects describe ${PROJECT_ID} --format 'value(projectNumber)')
export CLOUDRUN=${PROJECTNUM}-compute@developer.gserviceaccount.com

gcloud secrets add-iam-policy-binding application_settings_etl \
  --member serviceAccount:${CLOUDRUN} --role roles/secretmanager.secretAccessor

gcloud secrets versions list application_settings_etl

# Allow access to components
export PROJECTNUM=$(gcloud projects describe ${PROJECT_ID} --format 'value(projectNumber)')
export CLOUDBUILD=${PROJECTNUM}@cloudbuild.gserviceaccount.com

# Build  Docker container
gcloud builds submit --tag gcr.io/$PROJECT_ID/opencovid2-etl
gcloud container images list

# Past secrets to the Docket container
gcloud secrets add-iam-policy-binding application_settings_etl \
  --member serviceAccount:${CLOUDBUILD} --role roles/secretmanager.secretAccessor

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member serviceAccount:${CLOUDBUILD} --role roles/cloudsql.client

# Run Migrations
gcloud builds submit --config cloudmigrate.yaml --substitutions _REGION=$REGION

# Django on Cloud Run
gcloud run deploy opencovid2-etl --platform managed --region $REGION \
  --image gcr.io/$PROJECT_ID/opencovid2-etl \
  --add-cloudsql-instances crawling-education:${REGION}:opencovid-db \
  --allow-unauthenticated