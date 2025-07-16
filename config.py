import os

# ImageRootDirectory = "/data/sftp/seudata/upload/transfer_temp"
ImageRootDirectory = "/home/seele/Desktop/WorkSpace/PipelineWorker/data"
ImageTransferTemp = ImageRootDirectory
ImageProcessedFilesArchive = os.path.join("/home/seele/Desktop/WorkSpace/PipelineWorker", "Processed_Files_Archive")

HNDBRootDirectory = "/home/seele/Desktop/WorkSpace/PipelineWorker/HNDB"
SamplePreparationDirectory = os.path.join(HNDBRootDirectory, "HNDB_files", "SamplePreparation")
Cell_ImagesDirectory = os.path.join(HNDBRootDirectory, "Cell_Images")
Cell_MIPsDirectory = os.path.join(HNDBRootDirectory, "Cell_MIPs")
PTRSB_DBDirectory = os.path.join(HNDBRootDirectory, "PTRSB_DB")
