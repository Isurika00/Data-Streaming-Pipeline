import subprocess

print("Running MongoDB data load script...")
subprocess.run("python mongo-db.py", shell=True, check=True)

print("Running MongoDB to PostgreSQL script...")
subprocess.run("python time-scale-db.py", shell=True, check=True)

print("Data pipeline execution completed successfully!")