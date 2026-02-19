# Git Repository Structure & Setup Guide

## 1. Recommended Structure: Git Submodules

To manage `gcp-handson` as a collection of independent projects ("assets") where each has its own Git history, the **Git Submodule** pattern is the best fit.

### Structure Overview

```text
gcp-handson/             (Main Repository - Container)
├── .gitmodules          (Tracks submodules)
├── cdc-architecture/    (Submodule - Specific Asset)
│   ├── .git/            (Independent History)
│   ├── main.py
│   └── ...
├── future-asset-A/      (Submodule)
└── future-asset-B/      (Submodule)
```

### Benefits

- **Independent History:** `cdc-architecture` commits are separate from the main repo.
- **Modularity:** You can clone just the asset you need, or the whole collection.
- **Clean Management:** `gcp-handson` only tracks _verisons_ (pointers) of the assets, keeping the main repo lightweight.

---

## 2. Setup Step-by-Step

I have already initialized the **local git repository** for the current workspace (`cdd-architecture`).

### Step 1: Create Repositories on GitHub

Go to [GitHub](https://github.com/new) and create **two** empty public/private repositories:

1.  `cdc-architecture`
2.  `gcp-handson`

### Step 2: Push the Asset Repo (cdc-architecture)

Run these commands in your _current_ terminal (checking you are inside `cdc-architecture` folder).
This is the step that actually **uploads your code** to GitHub.

```bash
# 1. Add remote origin (Replace URL with your actual GitHub URL)
# 1. Link your local folder to the remote GitHub repo
git remote add origin https://github.com/jinseo-jang/gcp-handson.git

# 2. Upload (Push) your files to GitHub
# This sends main.py, README.md, etc. to the repository.
git branch -M main
git push -u origin main
```

### Step 3: Configure the Main Repo (gcp-handson)

Move to a parent directory (e.g., `~/handson`) to set up the container.

```bash
# 1. Clone or Init the Main Container Repo
# [Explanation] Creates a new parent directory `gcp-handson` for the main repository.
cd ..
mkdir gcp-handson
cd gcp-handson
git init

# 2. Add the cdc-architecture as a Submodule
# [Explanation] This downloads the code from the remote URL into a NEW folder `cdc-architecture` inside `gcp-handson`.
# It links the Remote Repo as a dependency of this Container Repo.
git submodule add https://github.com/jinseo-jang/gcp-handson.git cdc-architecture

# 3. Commit the change
git add .gitmodules cdc-architecture
git commit -m "Add cdc-architecture asset"

# 4. Set Remote for Container Repo (CRITICAL)
# You need a SEPARATE repository for this container (e.g., gcp-handson-container)
# Do NOT use the same URL as the asset repo above.
# git remote add origin https://github.com/jinseo-jang/YOUR_CONTAINER_REPO.git
# git push -u origin main
```

Now, your `gcp-handson` repository contains `cdc-architecture` as a distinct module. To add future assets, simply repeat the process: create a separate repo for the asset, then `git submodule add` it to `gcp-handson`.
