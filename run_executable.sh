#!/bin/bash

#input_file_glob_="/run/media/tanmaypani/Samsung-1tb/data/JEWEL/pp200GeV_dijet.root"
input_file_glob_="/run/media/tanmaypani/Samsung-1tb/data/JEWEL/0010_AuAu200GeV_dijet_withoutRec_*.root"
tree_name_="hepmc2Tree"
output_dir_="$PWD/output"
batch_size_=100000

./build/arrowjets_from_rootevents "$input_file_glob_" "$tree_name_" "$output_dir_" "$batch_size_"