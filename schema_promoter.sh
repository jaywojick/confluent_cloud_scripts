#!/bin/bash

# Confluent Cloud Schema Promotion Script
# This script promotes schemas from source to target environment in Confluent Cloud

# Exit on error
set -e

# Log file
LOG_FILE="schema_promotion.log"

# Get configuration via user prompts
echo "Please provide the configuration values:"

# Source Schema Registry URL
read -p "Source Schema Registry URL: " SOURCE_SR_URL
SOURCE_SR_URL=${SOURCE_SR_URL:-"https://dev-psrc-xxxxx.region.aws.confluent.cloud"}

# Target Schema Registry URL
read -p "Target Schema Registry URL: " TARGET_SR_URL
TARGET_SR_URL=${TARGET_SR_URL:-"https://qa-psrc-xxxxx.region.aws.confluent.cloud"}

# Source API credentials
read -p "Source API Key: " SOURCE_API_KEY
read -s -p "Source API Secret: " SOURCE_API_SECRET
echo ""

# Target API credentials
read -p "Target API Key: " TARGET_API_KEY
read -s -p "Target API Secret: " TARGET_API_SECRET
echo ""

# Compatibility level with default
read -p "Compatibility Level [BACKWARD]: " input_compatibility
COMPATIBILITY_LEVEL=${input_compatibility:-"BACKWARD"}

# Set to 1 to enable debug output
DEBUG=0

# Initialize log file
echo "$(date) - Starting schema promotion" > "$LOG_FILE"

# Function to log messages
log() {
    local message="$1"
    local level="${2:-INFO}"
    echo "$(date) - $level - $message" >> "$LOG_FILE"
    
    if [[ "$level" == "ERROR" || "$DEBUG" -eq 1 ]]; then
        echo "$(date) - $level - $message"
    fi
}

# Function to make API requests
make_request() {
    local method="$1"
    local url="$2"
    local api_key="$3"
    local api_secret="$4"
    local data="$5"
    
    if [[ -n "$data" ]]; then
        if [[ "$DEBUG" -eq 1 ]]; then
            log "Request data: $data" "DEBUG"
        fi
        
        response=$(curl -s -X "$method" "$url" \
            -u "${api_key}:${api_secret}" \
            -H "Content-Type: application/json" \
            -d "$data" 2>> "$LOG_FILE")
    else
        response=$(curl -s -X "$method" "$url" \
            -u "${api_key}:${api_secret}" \
            -H "Content-Type: application/json" 2>> "$LOG_FILE")
    fi
    
    http_code=$?
    
    if [[ "$DEBUG" -eq 1 ]]; then
        log "Response: $response" "DEBUG"
    fi
    
    if [[ "$http_code" -ne 0 ]]; then
        log "HTTP request failed with error code $http_code" "ERROR"
        return 1
    fi
    
    echo "$response"
}

# Get all subjects from source environment
get_subjects() {
    local url="$1/subjects"
    local api_key="$2"
    local api_secret="$3"
    
    log "Fetching subjects from $url"
    
    response=$(make_request "GET" "$url" "$api_key" "$api_secret")
    
    if [[ "$response" == *"error_code"* ]]; then
        log "Failed to get subjects: $response" "ERROR"
        return 1
    fi
    
    echo "$response"
}

# Get the latest schema for a subject
get_latest_schema() {
    local url="$1/subjects/$2/versions/latest"
    local api_key="$3"
    local api_secret="$4"
    
    log "Fetching latest schema for subject $2"
    
    response=$(make_request "GET" "$url" "$api_key" "$api_secret")
    
    if [[ "$response" == *"error_code"* ]]; then
        log "Failed to get schema for $2: $response" "ERROR"
        return 1
    fi
    
    echo "$response"
}

# Check if schema is compatible with target environment
check_compatibility() {
    local url="$1/compatibility/subjects/$2/versions/latest"
    local api_key="$3"
    local api_secret="$4"
    local schema="$5"
    
    log "Checking compatibility for subject $2"
    
    response=$(make_request "POST" "$url" "$api_key" "$api_secret" "$schema")
    
    if [[ "$response" == *"error_code"* ]]; then
        log "Compatibility check failed for $2: $response" "ERROR"
        return 1
    fi
    
    if [[ "$response" == *"is_compatible\":true"* ]]; then
        return 0
    else
        return 1
    fi
}

# Set compatibility level for a subject
set_compatibility() {
    local url="$1/config/$2"
    local api_key="$3"
    local api_secret="$4"
    local compatibility="$5"
    
    log "Setting compatibility level to $compatibility for subject $2"
    
    data="{\"compatibility\":\"$compatibility\"}"
    
    response=$(make_request "PUT" "$url" "$api_key" "$api_secret" "$data")
    
    if [[ "$response" == *"error_code"* ]]; then
        log "Failed to set compatibility for $2: $response" "ERROR"
        return 1
    fi
    
    return 0
}

# Register schema in target environment
register_schema() {
    local url="$1/subjects/$2/versions"
    local api_key="$3"
    local api_secret="$4"
    local schema="$5"
    
    log "Registering schema for subject $2"
    
    response=$(make_request "POST" "$url" "$api_key" "$api_secret" "$schema")
    
    if [[ "$response" == *"error_code"* ]]; then
        log "Failed to register schema for $2: $response" "ERROR"
        return 1
    fi
    
    log "Successfully registered schema for $2: $response"
    return 0
}

# Extract schema from JSON response
extract_schema() {
    local json="$1"
    
    # Use jq if available for better JSON parsing
    if command -v jq &> /dev/null; then
        schema=$(echo "$json" | jq -c .)
    else
        # Simple extraction fallback (less reliable)
        schema="$json"
    fi
    
    echo "$schema"
}

# Main function to promote schemas
promote_schemas() {
    local subjects_json="$1"
    
    # Use jq if available for better JSON parsing
    if command -v jq &> /dev/null; then
        subjects=$(echo "$subjects_json" | jq -r '.[]')
    else
        # Simple extraction fallback (less reliable)
        subjects=$(echo "$subjects_json" | tr -d '[]"' | tr ',' '\n')
    fi
    
    success_count=0
    failure_count=0
    skip_count=0
    
    for subject in $subjects; do
        log "Processing subject: $subject"
        
        # Get latest schema from source
        source_schema_json=$(get_latest_schema "$SOURCE_SR_URL" "$subject" "$SOURCE_API_KEY" "$SOURCE_API_SECRET")
        if [[ $? -ne 0 ]]; then
            log "Skipping subject $subject due to error fetching source schema" "ERROR"
            ((failure_count++))
            continue
        fi
        
        # Extract schema field from response
        if command -v jq &> /dev/null; then
            schema_to_register=$(echo "$source_schema_json" | jq -c '{schema: .schema}')
            schema_string=$(echo "$source_schema_json" | jq -r '.schema')
        else
            # Fallback extraction (very basic)
            schema_to_register="{\"schema\":$(echo "$source_schema_json" | grep -o '\"schema\":\"[^\"]*\"' | cut -d ':' -f2-)}"
            schema_string=$(echo "$source_schema_json" | grep -o '\"schema\":\"[^\"]*\"' | cut -d ':' -f2- | tr -d '"')
        fi
        
        # Check if schema already exists in target
        target_schema_json=$(get_latest_schema "$TARGET_SR_URL" "$subject" "$TARGET_API_KEY" "$TARGET_API_SECRET" 2>/dev/null)
        
        # If schema exists and is the same, skip it
        if [[ $? -eq 0 && "$target_schema_json" == *"$schema_string"* ]]; then
            log "Schema for $subject is already up to date, skipping"
            ((skip_count++))
            continue
        fi
        
        # Set compatibility level
        set_compatibility "$TARGET_SR_URL" "$subject" "$TARGET_API_KEY" "$TARGET_API_SECRET" "$COMPATIBILITY_LEVEL"
        
        # Check compatibility
        check_compatibility "$TARGET_SR_URL" "$subject" "$TARGET_API_KEY" "$TARGET_API_SECRET" "$schema_to_register"
        compatible=$?
        
        if [[ "$compatible" -eq 0 ]]; then
            # Register schema
            register_schema "$TARGET_SR_URL" "$subject" "$TARGET_API_KEY" "$TARGET_API_SECRET" "$schema_to_register"
            if [[ $? -eq 0 ]]; then
                log "Successfully promoted schema for $subject"
                ((success_count++))
            else
                log "Failed to register schema for $subject" "ERROR"
                ((failure_count++))
            fi
        else
            log "Schema for $subject is incompatible with target" "ERROR"
            ((failure_count++))
        fi
        
        # Add delay to avoid rate limiting
        sleep 0.5
    done
    
    log "Schema promotion completed. Successful: $success_count, Failed: $failure_count, Skipped: $skip_count"
    echo -e "\nSchema Promotion Results:"
    echo "Successful: $success_count"
    echo "Failed: $failure_count"
    echo "Skipped: $skip_count"
    echo -e "\nCheck $LOG_FILE for detailed information"
}

# Main execution

# Prompt for specific subjects or use all
read -p "Do you want to specify subjects? (y/n): " specify_subjects

if [[ "$specify_subjects" == "y" || "$specify_subjects" == "Y" ]]; then
    echo "Enter subjects (space-separated):"
    read subject_input
    
    # Convert input to array
    read -a subject_array <<< "$subject_input"
    
    # Format for JSON
    subjects_list="["
    for subject in "${subject_array[@]}"; do
        subjects_list+="\"$subject\","
    done
    subjects_list=${subjects_list%,}"]"
    
    log "Using provided subjects: $subjects_list"
    promote_schemas "$subjects_list"
else
    # Get all subjects from source
    log "Fetching all subjects from source environment"
    subjects_json=$(get_subjects "$SOURCE_SR_URL" "$SOURCE_API_KEY" "$SOURCE_API_SECRET")
    
    if [[ $? -eq 0 ]]; then
        promote_schemas "$subjects_json"
    else
        log "Failed to fetch subjects from source environment" "ERROR"
        exit 1
    fi
fi
