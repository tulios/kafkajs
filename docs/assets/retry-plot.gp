#!/usr/bin/env gnuplot
# gnuplot file for explaining the retry function
#
# $ gnuplot -c retry-plot.gnuplot
#

set style line 101 lc rgb '#808080' lt 1 lw 1
set border 3 front ls 101
set tics nomirror out scale 0.75
set format '%g'

set style line 102 lc rgb '#808080' lt 0 lw 1
set grid back ls 102

set style line 1 lc rgb '#0060ad' lt 1 lw 2 pt 7 pi -1 ps 1.1
set pointintervalbox 2

between(x,y) = rand(0) * (x - y) + y

fmax(n) = (n<1) ? 300 : fmax(n-1) * (1 - 0.2) * 2
fmin(n) = (n<1) ? 300 : fmin(n-1) * (1 + 0.2) * 2

set xlabel 'Retries'
set ylabel 'Wait duration (ms)'
set xtics font "Arial,16"
set terminal svg enhanced size 800, 600 dynamic
set output 'retry-plot.svg'
set sample 6

plot [x=0:5] fmin(x) title "Max" with linespoints linestyle 1 lw 2, fmax(x) title "Min" with linespoints linestyle 2 lw 2
