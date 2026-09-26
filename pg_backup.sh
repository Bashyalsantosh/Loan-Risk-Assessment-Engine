#!/usr/bin/env bash
# ==============================================================================
# Automated PostgreSQL Base Snapshot & WAL Retention Script
# ==============================================================================
set -euo pipefail

# Configuration Parameters
BACKUP_DIR="/var/backups/postgres"
BASE_BACKUP_DIR="${BACKUP_DIR}/base_backups"
WAL_ARCHIVE_DIR="${BACKUP_DIR}/wal_archive"
RETENTION_DAYS=7
PGUSER="postgres"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_PATH="${BASE_BACKUP_DIR}/base_${TIMESTAMP}"
LOG_FILE="${BACKUP_DIR}/backup.log"

# Log message function
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "${LOG_FILE}"
}

# Pre-execution checks
mkdir -p "${BASE_BACKUP_DIR}" "${WAL_ARCHIVE_DIR}"
log "START: Automated PostgreSQL Physical Base Snapshot Execution"

# Execute physical base backup using pg_basebackup
log "Executing pg_basebackup to path: ${BACKUP_PATH}..."
if sudo -u "${PGUSER}" pg_basebackup \
    -D "${BACKUP_PATH}" \
    -F tar \
    -z \
    -P \
    -X stream \
    -c fast \
    -l "base_backup_${TIMESTAMP}"; then
    log "SUCCESS: Base snapshot created successfully."
else
    log "ERROR: pg_basebackup failed!"
    exit 1
fi

# Cleanup old base backups beyond retention policy
log "Cleaning up base backups older than ${RETENTION_DAYS} days..."
find "${BASE_BACKUP_DIR}" -maxdepth 1 -type f -name "base_*.tar.gz" -mtime +${RETENTION_DAYS} -exec rm -vf {} \; | tee -a "${LOG_FILE}"

# Purge redundant WAL files using pg_archivecleanup
# Find the oldest active backup tar file and extract its location
OLDEST_BACKUP=$(ls -1d ${BASE_BACKUP_DIR}/base_* 2>/dev/null | head -n 1)
if [ -n "${OLDEST_BACKUP}" ]; then
    log "Purging WAL archives no longer needed by oldest remaining backup..."
    # Identify the earliest required WAL segment file
    START_WAL=$(pg_controldata "${OLDEST_BACKUP}" 2>/dev/null | grep "Latest checkpoint's REDO WAL file" | awk '{print $NF}')
    if [ -n "${START_WAL}" ]; then
        sudo -u "${PGUSER}" pg_archivecleanup "${WAL_ARCHIVE_DIR}" "${START_WAL}"
        log "Purged WAL logs prior to segment: ${START_WAL}"
    fi
fi

log "COMPLETED: Backup cycle finished successfully."
