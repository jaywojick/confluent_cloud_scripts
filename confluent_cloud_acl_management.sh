#!/bin/bash

# Confluent ACL Management Script
# This script provides functions to manage Confluent Platform ACL rules

# Color codes for better readability  
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'  
NC='\033[0m' # No Color

# Function to validate and select environment
validate_and_select_environment() {
    echo -e "${YELLOW}Listing Available Environments:${NC}"
    confluent environment list

    # Prompt to confirm current environment
    read -p "Are you in the correct environment? (y/n): " env_confirm
    
    if [[ "$env_confirm" =~ ^[Nn]$ ]]; then
        # If not in correct environment, guide user to select
        read -p "Enter the Environment ID you want to use: " selected_env
        
        # Use the selected environment
        confluent environment use "$selected_env"
        
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}Environment $selected_env selected successfully.${NC}"
        else
            echo -e "${RED}Failed to select environment $selected_env.${NC}"
            return 1
        fi
    fi
}

# Function to validate and select kafka cluster
validate_and_select_cluster() {
    echo -e "${YELLOW}Listing Available Kafka Clusters:${NC}"
    confluent kafka cluster list

    # Prompt to confirm current cluster
    read -p "Are you connected to the correct Kafka cluster? (y/n): " cluster_confirm
    
    if [[ "$cluster_confirm" =~ ^[Nn]$ ]]; then
        # If not in correct cluster, guide user to select
        read -p "Enter the Kafka Cluster ID you want to use: " selected_cluster
        
        # Use the selected cluster  
        confluent kafka cluster use "$selected_cluster"
        
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}Kafka Cluster $selected_cluster selected successfully.${NC}"
        else
            echo -e "${RED}Failed to select Kafka Cluster $selected_cluster.${NC}"
            return 1
        fi
    fi  
}

# Function to display script usage
usage() {
    echo "Confluent ACL Management Script"
    echo "Usage:"
    echo "  $0 list                 - List current ACL rules"
    echo "  $0 create               - Create a new ACL rule"
    echo "  $0 delete               - Delete an existing ACL rule"
    echo "  $0 env                  - Select and manage environments"
    echo "  $0 help                 - Show this help message"
}

# Function to list ACL rules
list_acl_rules() {
    echo -e "${YELLOW}Current ACL Rules:${NC}"
    confluent kafka acl list
}

# Function to create an ACL rule  
create_acl_rule() {
    # First, list current ACL rules
    echo -e "${YELLOW}Current ACL Rules:${NC}"
    confluent kafka acl list
    
    # Check if a principal was passed as an argument
    if [ -n "$1" ]; then
        principal="$1"
        echo -e "${YELLOW}Using Service Account: $principal${NC}"
    else
        # Prompt for ACL rule details
        read -p "Enter Service Account (e.g., sa-xxxxxx): " principal
    fi
    read -p "Enter Resource Type (topic/consumer-group/cluster-scope): " resource_type
    
    # Additional listing based on resource type
    if [[ "$resource_type" == "topic" ]]; then
        echo -e "${YELLOW}Available Topics:${NC}"
        confluent kafka topic list
    fi
    
    # Ensure full resource type flag
    case "$resource_type" in
        topic)
            resource_flag="--topic"
            ;;
        "consumer-group")  
            resource_flag="--consumer-group"
            ;;
        "cluster-scope")
            # Cluster-scope rules don't need a resource flag
            resource_flag=""
            resource_name=""
            ;;
        *)
            echo -e "${RED}Invalid resource type.${NC}"
            return 1
            ;;
    esac
    
    # Prompt for resource name only for non-cluster-scope
    if [[ "$resource_type" != "cluster-scope" ]]; then
        read -p "Enter Resource Name (use '*' for wildcard, or specific name): " resource_name
    fi
    
    read -p "Enter Operations (comma-separated, e.g., read,write,describe): " operations
    read -p "Enter Permission Type (allow/deny): " permission_type
    
    # Validate permission type
    if [[ ! "$permission_type" =~ ^(allow|deny)$ ]]; then
        echo -e "${RED}Invalid permission type. Must be 'allow' or 'deny'.${NC}"
        return 1
    fi
    
    # Validate operations for cluster-scope
    if [[ "$resource_type" == "cluster-scope" ]]; then
        # Restrict operations for cluster-scope to only read and describe
        valid_cluster_ops=("read" "describe")  
        is_valid_op=false
        for op in "${valid_cluster_ops[@]}"; do
            if [[ "$operations" == "$op" ]]; then
                is_valid_op=true
                break
            fi
        done
        
        if [[ "$is_valid_op" == false ]]; then
            echo -e "${RED}For cluster-scope, operations must be either 'read' or 'describe'.${NC}"
            return 1
        fi
    fi
    
    # Construct and execute the ACL rule creation command
    if [[ "$resource_type" == "cluster-scope" ]]; then
        # Special handling for cluster-scope
        cmd="confluent kafka acl create --$permission_type --service-account $principal --operations $operations --cluster-scope"
    elif [[ "$resource_name" == "*" ]]; then
        cmd="confluent kafka acl create --$permission_type --service-account $principal --operations $operations $resource_flag '*'"  
    else
        cmd="confluent kafka acl create --$permission_type --service-account $principal --operations $operations $resource_flag '$resource_name'"
    fi
    
    echo -e "${YELLOW}Executing: $cmd${NC}"
    
    # Use bash -c to execute the command to avoid parsing issues
    bash -c "$cmd"
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}ACL rule created successfully.${NC}"
        
        # List current ACL rules after successful creation
        echo -e "\n${YELLOW}Updated ACL Rules:${NC}"
        confluent kafka acl list
        
        # Ask if another ACL is needed for this user
        read -p "Do you need another ACL for $principal? (y/n): " another_acl
        if [[ "$another_acl" =~ ^[Yy]$ ]]; then
            read -p "Create or Delete ACL? (c/d): " acl_action
            if [[ "$acl_action" =~ ^[Cc]$ ]]; then
                create_acl_rule "$principal"
            elif [[ "$acl_action" =~ ^[Dd]$ ]]; then
                delete_acl_rule "$principal"
            else
                echo -e "${RED}Invalid action. Returning to main menu.${NC}"
            fi
        fi
    else
        echo -e "${RED}Failed to create ACL rule.${NC}"
    fi
}

# Function to delete an ACL rule
delete_acl_rule() {
    # First, list current ACL rules
    echo -e "${YELLOW}Current ACL Rules:${NC}"
    confluent kafka acl list
    
    # Check if a principal was passed as an argument
    if [ -n "$1" ]; then
        principal="$1"
        echo -e "${YELLOW}Using Service Account: $principal${NC}"
    else
        # Prompt for rule details to delete
        read -p "Enter Service Account (e.g., sa-xxxxxx): " principal
    fi
    read -p "Enter Resource Type (topic/consumer-group/cluster-scope): " resource_type
    
    # Additional listing based on resource type
    if [[ "$resource_type" == "topic" ]]; then
        echo -e "${YELLOW}Available Topics:${NC}"
        confluent kafka topic list
    fi
    
    # Ensure full resource type flag
    case "$resource_type" in  
        topic)
            resource_flag="--topic"
            ;;
        "consumer-group")
            resource_flag="--consumer-group"
            ;;
        "cluster-scope")
            # Cluster-scope rules don't need a resource flag
            resource_flag=""
            resource_name=""
            ;;
        *)  
            echo -e "${RED}Invalid resource type.${NC}"
            return 1  
            ;;
    esac
    
    # Prompt for resource name only for non-cluster-scope
    if [[ "$resource_type" != "cluster-scope" ]]; then
        read -p "Enter Resource Name (use '*' for wildcard, or specific name): " resource_name
    fi
    
    read -p "Enter Operations (comma-separated, e.g., read,write,describe): " operations
    read -p "Enter Permission Type (allow/deny): " permission_type
    
    # Validate permission type
    if [[ ! "$permission_type" =~ ^(allow|deny)$ ]]; then  
        echo -e "${RED}Invalid permission type. Must be 'allow' or 'deny'.${NC}"
        return 1
    fi
    
    # Validate operations for cluster-scope
    if [[ "$resource_type" == "cluster-scope" ]]; then
        # Restrict operations for cluster-scope to only read and describe
        valid_cluster_ops=("read" "describe")
        is_valid_op=false
        for op in "${valid_cluster_ops[@]}"; do
            if [[ "$operations" == "$op" ]]; then
                is_valid_op=true
                break
            fi
        done
        
        if [[ "$is_valid_op" == false ]]; then
            echo -e "${RED}For cluster-scope, operations must be either 'read' or 'describe'.${NC}"
            return 1
        fi
    fi
    
    # Construct and execute the ACL rule deletion command
    if [[ "$resource_type" == "cluster-scope" ]]; then
        # Special handling for cluster-scope
        cmd="confluent kafka acl delete --$permission_type --service-account $principal --operations $operations --cluster-scope"
    elif [[ "$resource_name" == "*" ]]; then
        cmd="confluent kafka acl delete --$permission_type --service-account $principal --operations $operations $resource_flag '*'"
    else  
        cmd="confluent kafka acl delete --$permission_type --service-account $principal --operations $operations $resource_flag '$resource_name'"
    fi
    
    echo -e "${YELLOW}Executing: $cmd${NC}"
    
    # Use bash -c to execute the command to avoid parsing issues
    bash -c "$cmd"
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}ACL rule deleted successfully.${NC}"
        
        # List updated ACL rules after successful deletion
        echo -e "\n${YELLOW}Updated ACL Rules:${NC}"
        confluent kafka acl list
        
        # Ask if another ACL is needed for this user
        read -p "Do you need another ACL for $principal? (y/n): " another_acl
        if [[ "$another_acl" =~ ^[Yy]$ ]]; then
            read -p "Create or Delete ACL? (c/d): " acl_action
            if [[ "$acl_action" =~ ^[Cc]$ ]]; then
                create_acl_rule "$principal"
            elif [[ "$acl_action" =~ ^[Dd]$ ]]; then
                delete_acl_rule "$principal"
            else
                echo -e "${RED}Invalid action. Returning to main menu.${NC}"
            fi
        fi
    else
        echo -e "${RED}Failed to delete ACL rule.${NC}"
        echo -e "${YELLOW}Suggestions:${NC}"
        echo "1. Verify the exact ACL rule parameters"
        echo "2. Double-check the service account, resource type, and operations"
        echo "3. Confirm the ACL rule exists before attempting to delete"  
    fi
}

# Interactive mode
interactive_mode() {
    # First, validate and select environment
    validate_and_select_environment
    
    # Then, validate and select Kafka cluster
    validate_and_select_cluster
    
    while true; do
        echo -e "\n${YELLOW}Confluent ACL Management${NC}"
        echo "1. List ACL Rules"
        echo "2. Create ACL Rule"
        echo "3. Delete ACL Rule"
        echo "4. Manage Environments"
        echo "5. Exit"
        
        read -p "Enter your choice (1-5): " choice
        
        case $choice in
            1)
                list_acl_rules
                ;;
            2)
                create_acl_rule
                ;;
            3)
                delete_acl_rule
                ;;
            4)
                validate_and_select_environment  
                validate_and_select_cluster
                ;;
            5)
                echo -e "${GREEN}Exiting Confluent ACL Management Script.${NC}"
                exit 0
                ;;
            *)
                echo -e "${RED}Invalid choice. Please try again.${NC}"  
                ;;
        esac
    done
}

# Main script logic
if [ $# -eq 0 ]; then
    # No arguments - start interactive mode  
    interactive_mode
else
    # Process command-line arguments
    case "$1" in
        list)
            validate_and_select_environment
            validate_and_select_cluster
            list_acl_rules
            ;;
        create)
            validate_and_select_environment
            validate_and_select_cluster
            create_acl_rule
            ;;
        delete)
            validate_and_select_environmentx
            validate_and_select_cluster
            delete_acl_rule
            ;;
        help)
            usage
            ;;
        *)
            echo -e "${RED}Invalid command. Use 'help' to see available options.${NC}"
            usage
            exit 1  
            ;;
    esac  
fi

exit 0