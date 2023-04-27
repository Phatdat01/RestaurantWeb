#!/bin/bash
git status
echo "Do you want to continue? (Y/N)"
read response
if [ "$response" == "Y" ] || [ "$response" == "y" ]; then
    echo "Continuing..."
    echo "Enter commit message:"
    read message
    git add .
    git commit -m "$message"
    git push
else
    echo "Exiting..."
    exit 0
fi
