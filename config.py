import os

# ImageRootDirectory = "/data/sftp/seudata/upload/transfer_temp"
ImageRootDirectory = "./data"
ImageTransferTemp = os.path.join(ImageRootDirectory, "transfer_temp")
ImageProcessedFilesArchive = "/PBshare/cloudForsftp/seusftp/upload/Processed_Files_Archive/"

HNDBRootDirectory = "./HNDB"
SamplePreparationDirectory = os.path.join(HNDBRootDirectory, "HNDB_files", "SamplePreparation")
Cell_ImageDirectory = os.path.join(HNDBRootDirectory, "Cell_Image")
ImageArchiveDirectory = os.path.join(HNDBRootDirectory, "PTRSB_DB", "ImageArchive")