use std::fs::{self, File};
use std::io::Write;
use std::io;
use std::path::{Path, PathBuf};
use zip::ZipArchive;
use std::thread;
use std::time::Duration;
use std::io::BufRead;
use std::sync::mpsc;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Extract files
    let zip_path = find_zip_file()?;
    let password = rpassword::prompt_password("Enter password: ")?;
    let mut extracted_files = extract_zip(&zip_path, Some(&password))?;

    let other_zips = find_other_zips()?;
    for zip in other_zips {
        let mut new_files = extract_zip(&zip, None)?;
        extracted_files.append(&mut new_files);
    }

    // Write paths to cleanup log
    let cleanup_log = "extracted_files.log";
    let mut log = File::create(cleanup_log)?;
    for path in &extracted_files {
        writeln!(log, "{}", path.display())?;
    }
    let log_path = PathBuf::from(cleanup_log);
    extracted_files.push(log_path);
    let handle = std::thread::spawn(move || {
        println!("Cleanup Process:");  // DEBUG
        wait_for_signal_and_cleanup(extracted_files)
    });

    // Keep main thread alive indefinitely
    loop {
        std::thread::sleep(Duration::from_secs(60));
    }
    
    // handle.join().unwrap()?;  // Alternative: wait for thread
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
    //println!("[Rust] Waiting for Python signal...");
    
    loop {
        // Check for confirmation file
        if Path::new("delete_confirmed.flag").exists() {
            println!("User confirmed deletion");
            // Perform cleanup
            for path in extracted_files {
                if path.exists() {
                    if path.is_dir() {
                        fs::remove_dir_all(&path)?;
                    } else {
                        fs::remove_file(&path)?;
                    }
                }
            }
            fs::remove_file("delete_confirmed.flag")?;
            break;
        }
        // Check for cancellation file
        else if Path::new("delete_cancelled.flag").exists() {
            println!("User cancelled deletion");
            fs::remove_file("delete_cancelled.flag")?;
            break;
        }
        thread::sleep(Duration::from_secs(1));
    }
    
    Ok(())
}

