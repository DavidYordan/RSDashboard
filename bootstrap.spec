# -*- mode: python ; coding: utf-8 -*-

import os

block_cipher = None

current_dir = os.getcwd()

datas = [
    (os.path.join(current_dir, 'dist', 'RSDashboard.exe'), 'dist'),
    (os.path.join(current_dir, 'img', 'RSDashboardResourceExtractor.ico'), 'img'),
]

a = Analysis(
    ['bootstrap.py'],
    pathex=[],
    binaries=[],
    datas=datas,
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='RSDashboardResourceExtractor',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    version='',
    icon='img/RSDashboardResourceExtractor.ico',
)
