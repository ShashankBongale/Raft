for itr in {1..50}
do
    go test -run 2B > output.txt
    grep -o -w "Passed" output.txt | wc -l
done
