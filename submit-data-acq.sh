#BSUB -J data-acq
#BSUB -o /zhome/e0/2/169222/Desktop/CSS_Project/hpc-logs/data-acq/%J.out
#BSUB -e /zhome/e0/2/169222/Desktop/CSS_Project/hpc-logs/data-acq/%J.err
#BSUB -q hpc
#BSUB -n 48
#BSUB -R "rusage[mem=1G]"
#BSUB -R "span[hosts=1]"
#BSUB -W 24:00
### -- send notification at start --
#BSUB -B
### -- send notification at completion--
#BSUB -N
# end of BSUB options

module load python3
python3 -m dataAcquistion.py
