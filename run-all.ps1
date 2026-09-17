# run-all.ps1 - starter consumers foerst, derefter web app
$root = $PSScriptRoot

# Consumers foerst, saa koeerne er bundet foer vi publisher
Start-Process powershell -ArgumentList "-NoExit","-Command","cd '$root\TourBooking.EmailService'; dotnet run"
Start-Process powershell -ArgumentList "-NoExit","-Command","cd '$root\TourBooking.BackOffice'; dotnet run"
Start-Process powershell -ArgumentList "-NoExit","-Command","cd '$root\TourBooking.AdminApp'; dotnet run"

# Giv dem et par sekunder til at forbinde og binde, foer web app starter
Start-Sleep -Seconds 3

# Web app sidst
Start-Process powershell -ArgumentList "-NoExit","-Command","cd '$root\TourBooking.WebApp'; dotnet watch"