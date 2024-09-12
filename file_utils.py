import pathlib
import shutil
import os
import datetime


class Directory:
    """
    Simple class for directories + operations.
    Init with an absolute path, or a path relative to the current working directory.
    If the dir does not yet exist, it will be created. Disable this by setting the 'create_dir' parameter to False.
    """

    def __init__(self, path: str, create_dir: bool = True):
        self.input_path_str = path
        self.create_dir = create_dir

        # check if the path is absolute
        if pathlib.Path(path).is_absolute():
            self.full = pathlib.Path(path)
        else:
            self.full = pathlib.Path.cwd() / path

        self.post_init()

    def post_init(self) -> None:
        '''
        Checks to see if this is actually a dir,
        or create it if create_dir is set to True.
        '''
        if not self.full.exists():
            if self.create_dir:
                self.create()
            else:
                raise FileNotFoundError(f"Directory {self.full} does not exist and create_dir is set to False.")
        if not self.full.is_dir():
            raise NotADirectoryError(f"Directory {self.full} is not a directory.")


    @property
    def files(self) -> list['File']:
        '''
        Returns all files in the dir as a list of File objects.
        '''
        return [File(self.full / file) for file in self.full.iterdir() if file.is_file()]

    @property
    def dirs(self, r: bool = False) -> list['Directory']:
        '''
        Returns a list of all dirs in this Directory as a list of Directory objects.
        If r is set to True, it will return all children dirs recursively.
        '''
        if not r:
            return [Directory(self, d) for d in self.full.iterdir() if d.is_dir()]
        if r:
            return [Directory(self, d) for d in self.full.rglob('*') if d.is_dir()]

    @property
    def exists(self) -> bool:
        return self.full.exists()

    @property
    def is_dir(self) -> bool:
        return self.full.is_dir()

    def create(self) -> None:
        try:
            self.full.mkdir(parents=True, exist_ok=False)
        except FileExistsError:
            pass

    def __eq__(self, other) -> bool:
        return self.full == other.full

    def __str__(self):
        return str(self.full)

    def __repr__(self):
        return f"DirPath('{self.input_path_str}') -> {self.full}"

class File:
    '''
    Simple class for files + operations
    Parameters:
        path: str or Path
            relative from the current working directory.
            OR
            absolute path to the file.
            Should always end with the filename including extension.
    '''
    def __init__(self, path: str | pathlib.Path):
        self._path_init_str = str(path)

        assert isinstance(path, str) or isinstance(path, pathlib.Path)

        if isinstance(path, pathlib.Path):
            self._path = path
            self._name = path.name
            self._extension = path.suffix
            self._dir = Directory(str(self._path.absolute().parent))
        elif isinstance(path, str):
            if '/' in path:
                self._name = path.rsplit('/', 1)[-1]
                self._dir = Directory(path.rsplit('/', 1)[0], create_dir=True)
            else:
                self._name = path
                self._dir = Directory(os.getcwd())

            self._extension = self._name.split('.')[-1]
            self._path = self._dir.full / self._name

    @property
    def exists(self) -> bool:
        return self._path.exists()

    @property
    def is_file(self) -> bool:
        return self._path.is_file()

    @property
    def path(self) -> pathlib.Path:
        return self._path
    
    @property
    def name(self) -> str:
        return self._name
    
    @property
    def extension(self) -> str:
        return self._extension
    
    @property
    def dir(self) -> Directory:
        return self._dir
    
    
    def copy(self, new_path: str) -> 'File':
        shutil.copy(self._path, new_path)
        return File(new_path)

    def move(self, new_path: str) -> 'File':
        shutil.move(self._path, new_path)
        return File(new_path)

    def rename(self, new_name: str) -> 'File':
        self._path = self._dir.full / new_name
        return File(self._path)

    def created(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(self._path.stat().st_birthtime)

    def modified(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(self._path.stat().st_mtime)

    def __eq__(self, other) -> bool:
        return self._path == other.path

    def __str__(self):
        return str(self._path)

    def __repr__(self):
        if self._path_init_str != str(self._path):
            return f"FilePath('{self._path_init_str}') -> {self._path}"
        else:
            return f"FilePath('{self._path}')"
