#!/bin/bash

set -e

mkdir -p $FILESTORE_MNT_DIR
mount -v -o nolock $FILESTORE_IP_ADDRESS:/$FILESTORE_SHARE_NAME $FILESTORE_MNT_DIR
mount

export PGPASSWORD="${POSTGRES_PASSWORD}"
member_id=$(psql -h "${POSTGRES_HOST}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -w -t -A -c "SELECT id FROM public.slk_member WHERE email = 'admin@localhost.local'")
echo "Member ID: $member_id"

if [[ $member_id =~ ^[0-9]+$ ]]; then
    # List all zip files
    zips=$(find /projects -type f -regex '.*\.zip')

    for zip in $zips; do
        echo "Unzipping $zip"
        project_uuid=$(cat /proc/sys/kernel/random/uuid)
        mkdir -p /projects/$project_uuid
        unzip -o -d /projects/$project_uuid $zip
        ids=$(find /projects/$project_uuid -mindepth 1 -maxdepth 1 -type d)
        size=$(echo "$ids" | wc -l)
        if [ "$size" -eq 1 ]; then
            for id in $ids; do
                project_id=$(basename "$id")
                if [[ $project_id =~ ^[0-9]+$ ]]; then
                    project_name=$(basename "$zip" | cut -d. -f1)
                    if [ ! -d $FILESTORE_MNT_DIR/$member_id/$project_id ]; then
                        echo "Project $project_name will be created with id $project_id and UUID $project_uuid"
                        psql -v ON_ERROR_STOP=1 -h "${POSTGRES_HOST}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -w <<-EOSQL
INSERT INTO public.slk_project (id, code, "name", description, repository, active, deleted, created, updated) 
OVERRIDING SYSTEM VALUE 
VALUES($project_id, '$project_uuid', '$project_name', '$project_name', '', true, false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
INSERT INTO public.slk_project_props (id, project, properties, created, updated) 
OVERRIDING SYSTEM VALUE 
VALUES($project_id, $project_id, '[{"envName":"__sl_ignore__"}]', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
INSERT INTO public.slk_project_member (project, "access", deployer, "member", registered, pat) VALUES ($project_id, 'ADMIN', true, $member_id, true, '');
EOSQL
                        mkdir -p $FILESTORE_MNT_DIR/$member_id/$project_id
                        cp -r $id/* $FILESTORE_MNT_DIR/$member_id/$project_id
                        rm -rf /projects/$project_uuid
                    else
                        echo "Project $project_id is already present in $FILESTORE_MNT_DIR/$member_id/"
                        rm -rf /projects/$project_uuid
                    fi
                else
                    echo "Project $project_id should consist of digits only"
                    rm -rf /projects/$project_uuid
                fi
            done
        else
            echo "Unzipped project should be placed in a single directory"
            rm -rf /projects/$project_uuid
        fi
    done
else
    exit 1
fi
