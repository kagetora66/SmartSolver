use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use zip::ZipArchive;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Extract main password-protected zip
    let zip_path = find_zip_file()?;
    println!("Found main zip file: {}", zip_path.display());

    let password = rpassword::prompt_password("Enter password: ")?;
    extract_zip(&zip_path, &password)?;

    // Find and extract additional zips
    let other_zips = find_other_zips()?;
    for zip in other_zips {
        println!("Found additional zip: {}", zip.display());
        extract_zip_unlocked(&zip)?;
    }

    println!("All extractions completed!");
    Ok(())
}

// Existing functions remain the same
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

// New function to find other zips
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

// Modified to handle both password and non-password extraction
fn extract_zip(zip_path: &Path, password: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Password verification logic remains the same
    {
        let file = File::open(zip_path)?;
        let mut archive = ZipArchive::new(file)?;
        let test_file = archive.by_index_decrypt(0, password.as_bytes());
        
        match test_file {
            Ok(Ok(_)) => (),
            Ok(Err(zip::result::InvalidPassword)) => return Err("Invalid password".into()),
            Err(e) => return Err(e.into()),
        }
    }

    extract_zip_contents(zip_path, Some(password))
}

// New function for non-password extraction
fn extract_zip_unlocked(zip_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    extract_zip_contents(zip_path, None)
}

// Common extraction logic
fn extract_zip_contents(zip_path: &Path, password: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open(zip_path)?;
    let mut archive = ZipArchive::new(file)?;

    for i in 0..archive.len() {
        let mut file = match password {
            Some(pwd) => match archive.by_index_decrypt(i, pwd.as_bytes()) {
                Ok(Ok(f)) => f,
                Ok(Err(_)) => return Err("Invalid password for file".into()),
                Err(e) => return Err(e.into()),
            },
            None => archive.by_index(i)?,
        };

        let outpath = file.mangled_name();
        if file.is_dir() {
            std::fs::create_dir_all(&outpath)?;
        } else {
            if let Some(parent) = outpath.parent() {
                if !parent.exists() {
                    std::fs::create_dir_all(parent)?;
                }
            }
            let mut outfile = File::create(&outpath)?;
            io::copy(&mut file, &mut outfile)?;
        }
    }
    Ok(())
}
