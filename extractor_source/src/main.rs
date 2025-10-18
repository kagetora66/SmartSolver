use std::fs::{self, File};
use std::io::Write;
use std::io;
use std::path::{Path, PathBuf};
use zip::ZipArchive;
use std::thread;
use std::time::Duration;
use std::io::BufRead;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Extract files
    let zip_path = if let Ok(path) = find_zip_file() {
    path
    } else {
    select_file().expect("No folder selected")
    }; 
    let mut extracted_files = loop {
        let password = rpassword::prompt_password("Enter password: ")?;
        match extract_zip(&zip_path, Some(&password)) {
            Ok(path) => break path,
            Err(_) => println!("Please type the password again"),
        }
    };
    let other_zips = find_other_zips()?;
    for zip in other_zips {
        if zip.file_name().unwrap() == extracted_files[0] {
        let mut new_files = extract_zip(&zip, None)?;
        extracted_files.append(&mut new_files);
        }
    }

    // Write paths to cleanup log
    let cleanup_log = "extracted_files.log";
    let mut log = File::create(cleanup_log)?;
    for path in &extracted_files {
        writeln!(log, "{}", path.display())?;
    }
    let log_path = PathBuf::from(cleanup_log);
    extracted_files.push(log_path);

    let result = wait_for_signal_and_cleanup(extracted_files);
    match result {
	Err(_) => println!("Failed to cleanup"),
	_ => println!("cleanedup succesfully"),
    };

    Ok(())
}
fn select_file() -> Option<PathBuf> {
    rfd::FileDialog::new()
    .set_title("Select a zip file containing full_log")
    .pick_file()
}

fn find_zip_file() -> Result<PathBuf, Box<dyn std::error::Error>> {
    for entry in std::fs::read_dir(".")? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("full_log") && name.ends_with(".zip") {
                    return Ok(path);
                }
            }
        }
    }
    Err("No 'full_log*.zip' file found".into())
}

fn find_other_zips() -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let mut zips = Vec::new();
    for entry in std::fs::read_dir(".")? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.ends_with(".zip") && !name.starts_with("full_log") {
                    zips.push(path);
                }
            }
        }
    }

    Ok(zips)
}

pub fn extract_zip(
    zip_path: &Path,
    password: Option<&str>,
) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let file = File::open(zip_path)?;
    let mut archive = ZipArchive::new(file)?;
    let length = archive.len();
    let mut extracted_paths = Vec::new();
    // Verify password if provided
    if let Some(pwd) = password {
        let test_file = archive.by_index_decrypt(0, pwd.as_bytes());
        match test_file {
            Ok(Ok(_)) => (),
            Ok(Err(_)) => return Err("Invalid password".into()),
            Err(e) => return Err(e.into()),
        }
    }

    for i in 0..archive.len() {
        let mut file = match password {
            Some(pwd) => match archive.by_index_decrypt(i, pwd.as_bytes()) {
                Ok(Ok(f)) => f,
                Ok(Err(_)) => return Err("Invalid password for file".into()),
                Err(e) => return Err(e.into()),
            },
            None => archive.by_index(i)?,
        };

        let outpath = sanitize_windows_path(&file.mangled_name());
        extracted_paths.push(outpath.clone());

        if file.is_dir() {
            std::fs::create_dir_all(&outpath)?;
        } else {
            if let Some(parent) = outpath.parent() {
                if !parent.exists() {
                    std::fs::create_dir_all(parent)?;
                    extracted_paths.push(parent.to_path_buf());
                }
            }
            let mut outfile = File::create(&outpath)?;
            io::copy(&mut file, &mut outfile)?;
        }
        let mut percent = ((i+1) * 100) / length;
        print!("\rArchive name: {} Percent Completed: {}", zip_path.display(), percent);
        io::stdout().flush().unwrap();
        thread::sleep(Duration::from_millis(5));
    }
    println!();
    //extraction confirmation
    if !zip_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .contains("full_log")
        {
            let _ = File::create("extracted_confirm")?;
        }

        Ok(extracted_paths)
}
fn sanitize_windows_path(path: &Path) -> PathBuf {
    let mut sanitized = PathBuf::new();
    for component in path.components() {
        if let Some(s) = component.as_os_str().to_str() {
            let cleaned = s.replace(|c: char|
            c == '<' || c == '>' || c == ':' ||
            c == '"' || c == '/' || c == '\\' ||
            c == '|' || c == '?' || c == '*',
            "_"
            );
            sanitized.push(cleaned);
        }
    }
    sanitized
}
fn wait_for_signal_and_cleanup(extracted_files: Vec<PathBuf>) -> io::Result<()> {
    let base_dir = std::env::current_dir()?;
    let python_done = base_dir.join("python_done.flag");
    let user_confirm = base_dir.join("delete_confirmed.flag");
    let user_cancel = base_dir.join("delete_cancelled.flag");
    let confirm = base_dir.join("extracted_confirm");
    // Wait for Python to finish first
    while !python_done.exists() {
        thread::sleep(Duration::from_secs(1));
    }
    println!("SmartSolver Finished");
    let mut paths = extracted_files;
    paths.sort_by_key(|p| -(p.components().count() as isize));
    // Then wait for user decision
    loop {
        if user_confirm.exists() {
            println!("Starting cleanup...");
            for path in paths {
                if path.exists() {
                    if path.is_dir() {
                        fs::remove_dir_all(&path)?;
                    } else {
                        fs::remove_file(&path)?;
                    }
                }
            }
            fs::remove_file(&user_confirm)?;
            fs::remove_file(&python_done)?;
            fs::remove_file(&confirm)?;
            break;
        }
        else if user_cancel.exists() {
            println!("Cleanup cancelled by user");
            fs::remove_file(&user_cancel)?;
            fs::remove_file(&python_done)?;
            fs::remove_file(&confirm)?;
            break;
        }
        thread::sleep(Duration::from_secs(1));
    }

    Ok(())
}

