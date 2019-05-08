package utils

func CheckForError(err error) {
	if err != nil {
		panic(err)
	}
}
