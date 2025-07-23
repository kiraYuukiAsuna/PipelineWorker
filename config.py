import os

CoreServerURL = "http://localhost:8000"

ImageRootDirectory = "/PBshare/cloudForsftp/seusftp/upload"
ImageTransferTemp = os.path.join(ImageRootDirectory, "transfer_temp")
ImageProcessedFilesArchive = os.path.join(
    ImageRootDirectory, "Processed_Files_Archive")

HNDBRootDirectory = "/home/seele/Desktop/WorkSpace/PipelineWorker/HNDB"
SamplePreparationDirectory = os.path.join(HNDBRootDirectory, "HNDB_files", "SamplePreparation")
Cell_ImagesDirectory = os.path.join(HNDBRootDirectory, "Cell_Images")
Cell_MIPsDirectory = os.path.join(HNDBRootDirectory, "Cell_MIPs")
PTRSB_DBDirectory = os.path.join(HNDBRootDirectory, "PTRSB_DB")
