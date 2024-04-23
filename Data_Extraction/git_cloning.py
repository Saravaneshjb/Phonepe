import git
repository_url='https://github.com/PhonePe/pulse.git'
destination_directory="D:\\Saravanesh Personal\\Guvi\\Capstone Projects\\Phonepe\\Data_Extraction\\Data"
git.Repo.clone_from(repository_url,destination_directory)