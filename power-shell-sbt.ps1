pushd ($env:HOMEDRIVE + $env:HOMEPATH + "\.")
pushd .\data\github\artefacts-2019_2023\etl-ml-pieces.scala\
$OutputEncoding = [console]::InputEncoding = [console]::OutputEncoding = New-Object System.Text.UTF8Encoding
sbt -v "-Dfile.encoding=UTF-8"
