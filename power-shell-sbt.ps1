pushd ($env:HOMEDRIVE + $env:HOMEPATH + "\.")
pushd .\data\github\artefacts-2019_2023\
# pushd .\etl-ml-pieces.scala\
pushd .\spark3-pieces.scala\
$OutputEncoding = [console]::InputEncoding = [console]::OutputEncoding = New-Object System.Text.UTF8Encoding
sbt -v --mem 4096
popd

# sbt -v "-Dfile.encoding=UTF-8"
# sbt -v "-Dfile.encoding=UTF-8" -J-Xms4g -J-Xmx8g -J-Xss12m
# C:\bin\java\jdk-8.0.372.7-hotspot\bin\java.exe -cp "~\AppData\Local\Coursier\cache\arc\https\github.com\sbt\sbt\releases\download\v1.8.3\sbt-1.8.3.zip\sbt\bin\sbt-launch.jar" xsbt.boot.Boot -v "-Dfile.encoding=UTF-8"
