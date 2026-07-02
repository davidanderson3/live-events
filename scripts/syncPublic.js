const fs = require('fs');
const path = require('path');

const pairs = [
  { src: 'js', dest: path.join('public', 'js'), isDir: true },
  { src: 'assets', dest: path.join('public', 'assets'), isDir: true },
  { src: 'data', dest: path.join('public', 'data'), isDir: true },
  { src: 'style.css', dest: path.join('public', 'style.css') },
  { src: 'index.html', dest: path.join('public', 'index.html') },
  { src: 'datasources-admin.html', dest: path.join('public', 'datasources-admin.html') },
  { src: 'backend-refresh.html', dest: path.join('public', 'backend-refresh.html') },
  { src: 'settings.html', dest: path.join('public', 'settings.html') },
  { src: 'report.html', dest: path.join('public', 'report.html') },
  { src: 'restore.html', dest: path.join('public', 'restore.html') },
  { src: 'service-worker.js', dest: path.join('public', 'service-worker.js') },
  { src: 'favicon.ico', dest: path.join('public', 'favicon.ico') },
  { src: 'smithsonian.png', dest: path.join('public', 'smithsonian.png') }
];

function copyFile(src, dest) {
  fs.copyFileSync(src, dest);
  console.log(`Copied ${src} -> ${dest}`);
}

function copyDir(srcDir, destDir) {
  fs.mkdirSync(destDir, { recursive: true });
  const entries = fs.readdirSync(srcDir, { withFileTypes: true });
  for (const entry of entries) {
    if (entry.name.startsWith('.')) {
      continue;
    }
    const srcPath = path.join(srcDir, entry.name);
    const destPath = path.join(destDir, entry.name);
    if (entry.isDirectory()) {
      copyDir(srcPath, destPath);
    } else if (entry.isFile()) {
      fs.copyFileSync(srcPath, destPath);
      console.log(`Copied ${srcPath} -> ${destPath}`);
    }
  }
}

pairs.forEach(({ src, dest, isDir }) => {
  const srcPath = path.resolve(src);
  const destPath = path.resolve(dest);
  if (!fs.existsSync(srcPath)) {
    console.warn(`Skip missing ${srcPath}`);
    return;
  }
  if (isDir) {
    copyDir(srcPath, destPath);
  } else {
    fs.mkdirSync(path.dirname(destPath), { recursive: true });
    copyFile(srcPath, destPath);
  }
});
