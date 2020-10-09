# Lithops on GCP Storage

Lithops with GCP Storage as storage backend.

### Installation

 1. [Login](https://console.cloud.google.com) to Google Cloud Console (or signup if you don't have an account).
 
 2. Create a new project. Name it `lithops` or similar.
 
 3. Navigate to *IAM & Admin* > *Service Accounts*.
 
 4. Click on *Create Service Account*. Name the service account `lithops-executor` or similar. Then click on *Create*.
 
 5. Add the following roles to the service account:
	 - Service Account User
	 - Cloud Functions Admin
	 - Pub/Sub Admin
	 - Storage Admin

 6. Click on *Continue*. Then, click on *Create key*. Select *JSON* and then *Create*. Download the JSON file to a secure location in you computer. Click *Done*.

 7. Navigate to *Storage* on the menu. Create a bucket and name it `lithops-data` or similar. Remember to update the corresponding Cloudbutton's config field with this bucket name.

### Configuration

8. Edit your cloudbutton config file and add the following keys:

```yaml
    lithops:
        storage_backend: gcp_storage

    gcp:
        project_name : <PROJECT_NAME>
        service_account : <SERVICE_ACCOUNT_EMAIL>
        credentials_path : <FULL_PATH_TO_CREDENTIALS_JSON>
        region : <REGION_NAME>
```

 - `project_name`: Project name introduced in step 2 (e.g. `lithops`)
 - `service_account`: Service account email of the service account created on step 4 (e.g. `lithops-executor@lithops.iam.gserviceaccount.com`)
 - `credentials_path`: **Absolute** path of your JSON key file downloaded in step 7 (e.g. `/home/myuser/lithops-invoker1234567890.json`)
 - `region`: Region of the bucket created at step 8. Functions and pub/sub queue will be created in the same region (e.g. `us-east1`)
 