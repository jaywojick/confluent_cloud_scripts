#!/bin/bash

# Script to manage Kafka topics using Confluent CLI
# Color codes for better readability  
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'  
NC='\033[0m' # No Color

# Function to validate and select environment
validate_and_select_environment() {
    printf "${YELLOW}Listing Available Environments:${NC}\n"
    confluent environment list || return 1
    
    # Prompt to confirm current environment
    printf "${YELLOW}Are you in the correct environment? (y/n): ${NC}"
    read env_confirm
    
    if [[ "$env_confirm" =~ ^[Nn]$ ]]; then
        # If not in correct environment, guide user to select
        printf "${GREEN}Enter the Environment ID you want to use: ${NC}"
        read selected_env
        
        # Use the selected environment
        if confluent environment use "$selected_env"; then
            printf "${GREEN}Environment $selected_env selected successfully.${NC}\n"
        else
            printf "${RED}Failed to select environment $selected_env.${NC}\n"
            return 1
        fi
    fi
    return 0
}

# Function to validate and select kafka cluster
validate_and_select_cluster() {
    printf "${YELLOW}Listing Available Kafka Clusters:${NC}\n"
    confluent kafka cluster list || return 1
    
    # Prompt to confirm current cluster
    printf "${YELLOW}Are you connected to the correct Kafka cluster? (y/n): ${NC}"
    read cluster_confirm
    
    if [[ "$cluster_confirm" =~ ^[Nn]$ ]]; then
        # If not in correct cluster, guide user to select
        printf "${GREEN}Enter the Kafka Cluster ID you want to use: ${NC}"
        read selected_cluster
        
        # Use the selected cluster  
        if confluent kafka cluster use "$selected_cluster"; then
            printf "${GREEN}Kafka Cluster $selected_cluster selected successfully.${NC}\n"
        else
            printf "${RED}Failed to select Kafka Cluster $selected_cluster.${NC}\n"
            return 1
        fi
    fi
    return 0
}

# Function to list all topics
list_topics() {
    printf "${YELLOW}===== Kafka Topics List =====${NC}\n"
    confluent kafka topic list
    return 0
}

# Function to display cleanup policy warning
display_cleanup_policy_warning() {
    echo "--------------------------------------------------------"
    printf "${YELLOW}IMPORTANT NOTE ABOUT CLEANUP POLICY:${NC}\n"
    printf "${YELLOW}This config designates the retention policy to use on log segments.${NC}\n"
    printf "${YELLOW}You cannot directly change cleanup.policy from 'delete' to 'compact,delete'.${NC}\n"
    printf "${YELLOW}To set cleanup.policy to 'compact,delete', you must first change from 'delete' to 'compact',${NC}\n"
    printf "${YELLOW}then change to 'compact,delete' in a separate update.${NC}\n"
    echo "--------------------------------------------------------"
}

# Delete Topics Function
delete_topic() {
    printf "${YELLOW}===== Kafka Topic Deleter =====${NC}\n"
    
    # Show list of available topics
    confluent kafka topic list
    
    # Ask about multiple topics or wildcard
    printf "${YELLOW}Do you want to delete multiple topics or use a wildcard? (y/n): ${NC}"
    read delete_multiple
    
    # Initialize topics array
    topics=()
    
    if [[ "$delete_multiple" =~ ^[Yy]$ ]]; then
        printf "${YELLOW}Enter topics to delete (space-separated or use wildcard '*'):${NC}\n"
        printf "${GREEN}Examples:${NC}\n"
        printf "  - Specific topics: test test1 test2\n"
        printf "  - Wildcard prefix: test*\n"
        printf "  - Wildcard suffix: *test\n"
        printf "  - Wildcard anywhere: *test*\n"
        read -p "Enter topics or wildcard pattern: " topic_input
        
        # Handle wildcard expansion
        if [[ "$topic_input" == *"*"* ]]; then
            # Use Confluent CLI to list matching topics
            topics=($(confluent kafka topic list | grep -E "$(echo "$topic_input" | sed 's/\*/.*/')" | awk '{print $1}'))
        else
            # Convert input to array
            read -a topics <<< "$topic_input"
        fi
    else
        # Prompt for a single topic name
        read -p "Enter the topic name to delete: " topic_name
        topics=("$topic_name")
    fi
    
 # Validate topics exist
    valid_topics=()
    for topic in "${topics[@]}"; do
        if confluent kafka topic describe "$topic" &> /dev/null; then
            valid_topics+=("$topic")
        else
            printf "${RED}Warning: Topic '$topic' does not exist. Skipping.${NC}\n"
        fi
    done
    
    # Confirm deletion
    if [ ${#valid_topics[@]} -eq 0 ]; then
        printf "${YELLOW}No valid topics to delete.${NC}\n"
        return 1
    fi
    
    printf "${RED}The following topics will be PERMANENTLY DELETED:${NC}\n"
    printf '%s\n' "${valid_topics[@]}"
    read -p "Are you ABSOLUTELY sure? Type 'YES' to confirm: " confirm
    
    if [[ "$confirm" == "YES" ]]; then
        # Delete topics
        delete_success=0
        delete_fail=0
        failed_topics=""
        
        for topic in "${valid_topics[@]}"; do
            if confluent kafka topic delete "$topic"; then
                printf "${GREEN}Topic '$topic' deleted successfully.${NC}\n"
                ((delete_success++))
            else
                printf "${RED}Failed to delete topic '$topic'.${NC}\n"
                ((delete_fail++))
                failed_topics+=" $topic"
            fi
        done
        
        # Summary
        printf "\n${YELLOW}Deletion Summary:${NC}\n"
        printf "${GREEN}Successfully deleted: $delete_success topics${NC}\n"
        if [ $delete_fail -gt 0 ]; then
            printf "${RED}Failed to delete: $delete_fail topics (${failed_topics# })${NC}\n"
        fi
    else
        printf "${YELLOW}Deletion cancelled.${NC}\n"
    fi
    
    return 0
}

# Update Topics Function
update_topic() {
    printf "${YELLOW}===== Kafka Topic Updater =====${NC}\n"
    
    # Show list of available topics
    printf "${YELLOW}Available Topics:${NC}\n"
    confluent kafka topic list
    printf "\n"
    
    # Ask about multiple topics
    printf "${YELLOW}Do you want to update multiple topics with the same configuration? (y/n): ${NC}"
    read update_multiple
    
    # Initialize topics array
    topics=()
    
    if [[ "$update_multiple" =~ ^[Yy]$ ]]; then
        # Prompt for multiple topic names
        printf "${YELLOW}Enter topic names (space-separated): ${NC}"
        read -a topics
    else
        # Prompt for a single topic name
        read -p "Enter the topic name to update: " topic_name
        topics=("$topic_name")
    fi
    
    # Show basic information for each topic that will be updated
    printf "\n${YELLOW}Topics that will be updated:${NC}\n"
    for topic in "${topics[@]}"; do
        # Check if topic exists
        if confluent kafka topic describe "$topic" &> /dev/null; then
            # Get topic description
            topic_desc=$(confluent kafka topic describe "$topic")
            
            # Extract basic info
            printf "\n${GREEN}==== Current Configuration for Topic '$topic' ====${NC}\n"
            printf "Topic Name: %s\n" "$topic"
            
            # Get partition count
            part_count=$(echo "$topic_desc" | grep -E "^PartitionCount:" | awk '{print $2}')
            if [ -z "$part_count" ]; then
                part_count=$(echo "$topic_desc" | grep -E "num.partitions" | awk -F'|' '{print $2}' | xargs | head -1)
            fi
            if [ -n "$part_count" ]; then
                printf "Partition Count: %s\n" "$part_count"
            fi
            
            # Get replication factor
            rep_factor=$(echo "$topic_desc" | grep -E "^ReplicationFactor:" | awk '{print $2}')
            if [ -n "$rep_factor" ]; then
                printf "Replication Factor: %s\n" "$rep_factor"
            fi
            
            # Show key configuration values
            printf "\n${YELLOW}Current Configuration Values:${NC}\n"
            echo "$topic_desc" | grep -E "cleanup.policy|delete.retention.ms|retention.ms|retention.bytes|max.message.bytes|min.insync.replicas" | awk -F'|' '{printf "%-40s:  %s\n", $1, $2}' | sed 's/^[ \t]*//'
            printf "\n"
        else
            printf "${RED}Topic '$topic' does not exist and will be skipped.${NC}\n"
        fi
    done
    
    # Use the first topic to inspect configurations for update choices
    first_topic="${topics[0]}"
    
    # Check if topic exists
    if ! confluent kafka topic describe "$first_topic" &> /dev/null; then
        printf "${RED}Error: Topic '$first_topic' does not exist.${NC}\n"
        return 1
    fi
    
    # Create temporary file to store topic description for later use
    temp_desc_file=$(mktemp)
    confluent kafka topic describe "$first_topic" > "$temp_desc_file"
    
    # Skip showing the full description of the first topic since we already showed its configuration
    # This prevents duplicate display of the first topic
    
    # Extract basic information (once only)
    printf "\n${YELLOW}Proceeding with updates. Using '$first_topic' as reference for configuration options.${NC}\n"
    
    # Create temporary file to store editable configurations
    temp_edit_file=$(mktemp)
    # Filter out only configurations that are editable (marked as false in the Read-Only column)
    # and explicitly exclude file.delete.delay.ms which is actually read-only
    grep -E "\|\s*false\s*\|$" "$temp_desc_file" | grep -v "file.delete.delay.ms" > "$temp_edit_file"
    
    # Check if we found any editable configurations
    if [ ! -s "$temp_edit_file" ]; then
        # Try an alternative method to find editable configurations, still excluding file.delete.delay.ms
        grep -v "true" "$temp_desc_file" | grep "|" | grep -v "Name\|----" | grep -v "file.delete.delay.ms" > "$temp_edit_file"
    fi
    
    continue_updating=true
    while $continue_updating; do
        # Display editable configurations
        printf "\n${YELLOW}Editable Configurations:${NC}\n"
        
        # Display configs with line numbers
        line_count=0
        while IFS= read -r line; do
            ((line_count++))
            config=$(echo "$line" | awk -F'|' '{print $1}' | sed 's/^[ \t]*//;s/[ \t]*$//')
            value=$(echo "$line" | awk -F'|' '{print $2}' | sed 's/^[ \t]*//;s/[ \t]*$//')
            printf "%d) %s = %s\n" "$line_count" "$config" "$value"
        done < "$temp_edit_file"
        
        # Add finish option
        printf "%d) Finish Updating\n" "$((line_count+1))"
        
        # Get user input for configuration to update
        read -p "Enter the number of the configuration to update: " config_number
        
        # Check if user wants to finish
        if [[ "$config_number" -eq $((line_count+1)) ]]; then
            break
        fi
        
        # Validate the selection
        if [[ "$config_number" -lt 1 || "$config_number" -gt "$line_count" ]]; then
            printf "${RED}Invalid configuration selection.${NC}\n"
            continue
        fi
        
        # Get the selected configuration line
        selected_line=$(sed -n "${config_number}p" "$temp_edit_file")
        
        # Extract configuration name and value
        selected_config=$(echo "$selected_line" | awk -F'|' '{print $1}' | sed 's/^[ \t]*//;s/[ \t]*$//')
        current_value=$(echo "$selected_line" | awk -F'|' '{print $2}' | sed 's/^[ \t]*//;s/[ \t]*$//')
        
        # Display the current value
        printf "${GREEN}Current value for $selected_config: $current_value${NC}\n"
        
        # Handle special configurations
        if [[ "$selected_config" == "num.partitions" ]]; then
            printf "${YELLOW}WARNING: Partition count can ONLY be increased, never decreased!${NC}\n"
            # Use the current value directly
            printf "${YELLOW}Current partition count: $current_value${NC}\n"
            read -p "Enter new partition count (must be higher than current count): " new_value
            
            # Validate partition count is a number
            if ! [[ "$new_value" =~ ^[0-9]+$ ]]; then
                printf "${RED}ERROR: Partition count must be a valid number.${NC}\n"
                continue
            fi
            
            # Validate partition count is not decreased
            if (( new_value < current_value )); then
                printf "${RED}ERROR: Cannot decrease partition count. Current count is $current_value.${NC}\n"
                continue
            fi
        elif [[ "$selected_config" == "cleanup.policy" ]]; then
            # Show cleanup policy selection menu
            printf "\nSelect cleanup policy:\n"
            echo "1) delete"
            echo "2) compact"
            echo "3) compact,delete"
            read -p "Enter choice (1-3): " cleanup_choice
            
            case $cleanup_choice in
                1) new_value="delete" ;;
                2) new_value="compact" ;;
                3) new_value="compact,delete" ;;
                *) printf "${RED}Invalid choice. Using current value.${NC}\n"
                   continue ;;
            esac
            
            # Display cleanup policy warning
            display_cleanup_policy_warning
            
            # If switching to compact or compact,delete, prompt for additional configurations
            if [[ "$new_value" =~ ^compact ]]; then
                # Configure delete.retention.ms
                printf "\n${YELLOW}Configure delete.retention.ms:${NC}\n"
                read -p "Enter delete.retention.ms value (default 86400000 ms): " delete_retention_ms
                delete_retention_ms=${delete_retention_ms:-86400000}
                
                # Note about file.delete.delay.ms being read-only
                printf "\n${YELLOW}Note: file.delete.delay.ms is read-only and cannot be modified.${NC}\n"
                
                # No longer including file.delete.delay settings here since it's read-only
                # Add these configurations to the update
                for topic in "${topics[@]}"; do
                    confluent kafka topic update "$topic" --config "delete.retention.ms=$delete_retention_ms"
                done
            fi
        else
            read -p "Enter new value for $selected_config: " new_value
        fi
        
        # Update each topic
        for topic in "${topics[@]}"; do
            # Check if topic exists
            if ! confluent kafka topic describe "$topic" &> /dev/null; then
                printf "${RED}Error: Topic '$topic' does not exist. Skipping.${NC}\n"
                continue
            fi
            
            # Update configuration
            update_output=$(confluent kafka topic update "$topic" --config "$selected_config=$new_value" 2>&1)
            if [ $? -eq 0 ]; then
                printf "${GREEN}Configuration '$selected_config' updated successfully for topic '$topic'.${NC}\n"
                # Display the updated configuration to verify
                printf "${YELLOW}Updated configuration:${NC}\n"
                confluent kafka topic describe "$topic" | grep "$selected_config"
            else
                printf "${RED}Failed to update configuration for topic '$topic':${NC}\n"
                printf "${RED}$update_output${NC}\n"
            fi
        done
        
        # Prompt to continue updating
        printf "\n${YELLOW}Do you want to update another configuration? (y/n): ${NC}"
        read continue_update
        
        if [[ ! "$continue_update" =~ ^[Yy]$ ]]; then
            continue_updating=false
        fi
    done
    
    # Clean up temporary files
    rm -f "$temp_desc_file" "$temp_edit_file"
    
    # At the end, show the complete updated topic description for all topics that were updated
    printf "\n${YELLOW}Final Topic Descriptions after updates:${NC}\n"
    for topic in "${topics[@]}"; do
        # Check if topic exists before trying to describe it
        if confluent kafka topic describe "$topic" &> /dev/null; then
            printf "\n${GREEN}==== Topic: $topic ====${NC}\n"
            confluent kafka topic describe "$topic"
            printf "\n${YELLOW}-------------------------------------${NC}\n"
        fi
    done
    
    return 0
}

# Create Topics Function
create_topic() {
    printf "${YELLOW}===== Kafka Topic Creator =====${NC}\n"
    
    # Show existing topics first
    printf "${YELLOW}Current Topics:${NC}\n"
    confluent kafka topic list
    printf "\n"
    
    # Ask about multiple topics
    printf "${YELLOW}Do you want to create multiple topics with the same configuration? (y/n): ${NC}"
    read create_multiple
    
    # Initialize topics array
    topics=()
    
    if [[ "$create_multiple" =~ ^[Yy]$ ]]; then
        # Prompt for multiple topic names
        printf "${YELLOW}Enter topic names (space-separated): ${NC}"
        read -a topics
    else
        # Prompt for a single topic name
        read -p "Enter the topic name: " topic_name
        topics=("$topic_name")
    fi
    
    # Display important information about cleanup policy
    display_cleanup_policy_warning
    
    # Ask about using defaults
    read -p "Use default settings? (y/n): " use_defaults
    
    # Custom configuration
    if [[ "$use_defaults" =~ ^[Nn]$ ]]; then
        # Partitions
        read -p "Number of partitions (default 6): " partitions
        partitions=${partitions:-6}
        
        # Cleanup policy
        printf "Select cleanup policy:\n"
        echo "1) delete"
        echo "2) compact"
        echo "3) compact,delete"
        read -p "Enter choice (1-3, default 1): " cleanup_choice
        
        case $cleanup_choice in
            2) cleanup_policy="compact" ;;
            3) cleanup_policy="compact,delete" ;;
            *) cleanup_policy="delete" ;;
        esac
        
        # Retention time
        read -p "Retention time in ms (default 604800000 - 7 days): " retention_ms
        retention_ms=${retention_ms:-604800000}
        
        # Retention bytes
        read -p "Retention bytes (-1 for no limit, default -1): " retention_bytes
        retention_bytes=${retention_bytes:--1}
        
        # Additional configurations for compact policies
        if [[ "$cleanup_policy" == "compact" || "$cleanup_policy" == "compact,delete" ]]; then
            # Delete retention configuration - critical for compacted topics
            default_delete_retention="86400000" # 24 hours in ms
            printf "\n${YELLOW}Configure delete.retention.ms:${NC}\n"
            printf "${GREEN}For compacted topics, this setting controls how long deleted records are retained.${NC}\n"
            printf "${GREEN}Recommended value: 86400000 (24 hours) or higher.${NC}\n"
            printf "${GREEN}Setting this too low can cause data loss during compaction.${NC}\n"
            read -p "Enter delete.retention.ms value (press enter for default 86400000 ms): " delete_retention_ms
            delete_retention_ms=${delete_retention_ms:-$default_delete_retention}
            
            # File delete delay configuration - only applicable during topic creation
            default_file_delete_delay="60000" # 60 seconds in ms
            printf "\n${YELLOW}Configure file.delete.delay.ms:${NC}\n"
            printf "${GREEN}This setting can only be configured at topic creation time.${NC}\n"
            printf "${GREEN}It controls how long deleted files are retained on disk after deletion.${NC}\n"
            read -p "Enter file.delete.delay.ms value (default 60000 ms): " file_delete_delay
            file_delete_delay=${file_delete_delay:-$default_file_delete_delay}
        fi
    fi
    
    # Create topics
    for topic in "${topics[@]}"; do
        # Check if topic already exists
        if confluent kafka topic describe "$topic" &> /dev/null; then
            printf "${RED}Error: Topic '$topic' already exists. Skipping.${NC}\n"
            continue
        fi
        
        # Prepare create command
        if [[ "$use_defaults" =~ ^[Yy]$ ]]; then
            # Default settings
            confluent kafka topic create "$topic"
        else
            # Custom configuration
            create_cmd=(confluent kafka topic create "$topic" 
                        --partitions "$partitions"
                        --config "cleanup.policy=$cleanup_policy"
                        --config "retention.ms=$retention_ms"
                        --config "retention.bytes=$retention_bytes")
            
            # Add compact policy configs if applicable
            if [[ "$cleanup_policy" == "compact" || "$cleanup_policy" == "compact,delete" ]]; then
                # Always include delete.retention.ms for compacted topics - it's critical
                create_cmd+=(--config "delete.retention.ms=$delete_retention_ms"
                             --config "file.delete.delay.ms=$file_delete_delay")
                
                printf "${GREEN}Added delete.retention.ms=$delete_retention_ms to topic configuration.${NC}\n"
                printf "${GREEN}Added file.delete.delay.ms=$file_delete_delay to topic configuration.${NC}\n"
            fi
            
            # Execute create command
            "${create_cmd[@]}"
        fi
        
        # Check if topic was created successfully
        if [ $? -eq 0 ]; then
            printf "${GREEN}Topic '$topic' created successfully.${NC}\n"
            
            # Show topic details
            confluent kafka topic describe "$topic"
        else
            printf "${RED}Failed to create topic '$topic'.${NC}\n"
        fi
    done
    
    return 0
}

# Describe Topic Function
describe_topic() {
    printf "${YELLOW}===== Kafka Topic Describe =====${NC}\n"
    
    # Show list of available topics
    confluent kafka topic list
    
    # Prompt for topic name
    read -p "Enter the topic name to describe (or leave blank to describe all): " topic_name
    
    if [ -z "$topic_name" ]; then
        printf "${YELLOW}Describing all topics in detail:${NC}\n"
        
        # Get list of all topics, excluding the header and footer rows
        topics=($(confluent kafka topic list | awk 'NR>1 && !/^-/{print $1}'))
        
        if [ ${#topics[@]} -eq 0 ]; then
            printf "${YELLOW}No topics found.${NC}\n"
        else
            for topic in "${topics[@]}"; do
                printf "\n${YELLOW}Details for topic '$topic':${NC}\n"
                confluent kafka topic describe "$topic"
                echo "-------------------------------------"
            done
        fi
    else
        # Check if topic exists
        if ! confluent kafka topic describe "$topic_name" &> /dev/null; then
            printf "${RED}Error: Topic '$topic_name' does not exist.${NC}\n"
            return 1
        fi
        
        # Describe the specific topic
        printf "${YELLOW}Details for topic '$topic_name':${NC}\n"
        confluent kafka topic describe "$topic_name"
    fi
    
    return 0
}

# Main function
main() {
    # Validate environment and cluster only once at the start
    validate_and_select_environment || return 1
    validate_and_select_cluster || return 1
    
    while true; do
        echo
        printf "${YELLOW}===== Kafka Topic Management Tool =====${NC}\n"
        echo "1) List all topics"
        echo "2) Create a new topic"
        echo "3) Update an existing topic"
        echo "4) Delete a topic"
        echo "5) Describe a topic"
        echo "6) Exit"
        echo
        read -p "Enter your choice (1-6): " operation_choice
        echo
        
        case $operation_choice in
            1) list_topics ;;
            2) create_topic ;;
            3) update_topic ;;
            4) delete_topic ;;
            5) describe_topic ;;
            6) printf "${GREEN}Exiting. Goodbye!${NC}\n"; return 0 ;;
            *) printf "${RED}Invalid choice. Please try again.${NC}\n" ;;
        esac
        
        echo
        echo "-------------------------------------"
        echo
    done
}

# Run the script
main
