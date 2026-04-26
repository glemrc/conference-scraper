import os
import sys
import subprocess

def create_executable():
    print("Preparing to build Conference Intelligence App...")
    
    # Check if pyinstaller is installed
    try:
        import PyInstaller
    except ImportError:
        print("PyInstaller not found. Installing now...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyinstaller"])
        
    main_script = os.path.join("src", "gui_app.py")
    
    if not os.path.exists(main_script):
        print(f"Error: Could not find main script at {main_script}")
        return

    # PyInstaller arguments
    # --noconsole: Don't show terminal window
    # --onefile: Bundle into a single executable
    # --add-data: Include necessary folders (data, logs, .env)
    
    # We will just package it. The user will be requested for PyInstaller.
    args = [
        "pyinstaller",
        "--name", "ConferenceIntelligence",
        "--windowed", # no-console
        "--onefile",
        # Adding src module files if pyinstaller misses them implicitly
        "--add-data", f"src{os.pathsep}src", 
        # Create empty folders for data and logs if they don't exist
        "--add-data", f".env{os.pathsep}.",
        main_script
    ]
    
    print("\nRunning PyInstaller command:")
    print(" ".join(args))
    
    subprocess.check_call(args)
    
    print("\nBuild complete. Check the 'dist' folder for the executable.")

if __name__ == "__main__":
    create_executable()
