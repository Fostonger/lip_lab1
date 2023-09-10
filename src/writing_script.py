from typing import Any

import matplotlib.pyplot as plt
import io
import csv
import sys, getopt, os
from PIL import Image

def fig2img(fig):
    buf = io.BytesIO()
    fig.savefig(buf)
    buf.seek(0)
    img = Image.open(buf)
    return img

def save_chart(fig, chart_name):
    img = fig2img(fig)
    img.save(chart_name + ".png")

def create_chart(chart_name, iteration, time_taken) -> Any:
    plt.title(chart_name)
    plt.xlabel('Iterations')
    plt.ylabel("Time taken")
    plt.plot(iteration, time_taken)

    return plt.gcf()

def read_csv_data(chart_filename, outputfile_dir):
    iteration = []
    time_taken = []
    with open(chart_filename, newline='') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        next(spamreader)
        for row in spamreader:
            iteration.append(int(row[0]))
            time_taken.append(float(row[1]))

    chart_filename_without_csv = chart_filename.split(".")[0].split('/')[-1]
    chart_name = ' '.join(chart_filename_without_csv.split("_"))
    fig = create_chart(chart_name, iteration, time_taken)

    save_chart(fig, outputfile_dir + chart_filename_without_csv)

def main(argv):
   inputfile = ''
   output_dir = ''
   opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])
   for opt, arg in opts:
      if opt == '-h':
         print ('test.py -i <inputfile> -o <outputfile>')
         sys.exit()
      elif opt in ("-i", "--ifile"):
         inputfile = arg
      elif opt in ("-o", "--ofile"):
         output_dir = arg
   if not os.path.exists(output_dir):
       os.makedirs(output_dir)
   read_csv_data(inputfile, output_dir)

if __name__ == "__main__":
   main(sys.argv[1:])
