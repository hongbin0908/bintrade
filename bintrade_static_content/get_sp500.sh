rm -rf SP500.csv
wget https://s3.amazonaws.com/quandl-static-content/Ticker+CSV%27s/Indicies/SP500.csv
awk -F"" '{for(i=1;i<=NF;i++){printf("%s\n",$i)}}' SP500.csv > tmp;
awk -F"," '{for(i=1;i<NF;i++){printf("%s\t",$i)}; printf("%s\n",$NF)}' tmp > tmp2
sed -i '1d' tmp2 
mv tmp2 SP500.csv
