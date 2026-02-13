# Run Matching System
python matcher.py

# Commit and Push script for Rent Matching System
$msg = "Update logic and data $(Get-Date -Format 'yyyy-MM-dd HH:mm')"
git add .
git commit -m $msg
git push origin main
Write-Host "Pushed to GitHub: $msg" -ForegroundColor Green
