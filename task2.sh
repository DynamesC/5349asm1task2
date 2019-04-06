if [ $# -ne 2 ]; then
    echo "Invalid number of parameters!"
    echo "Usage: ./tag_driver.sh [input_location] [output_location]"
    exit 1
fi


spark-submit  \
--master local[2] \
task2.py
--input $1
--output $2
