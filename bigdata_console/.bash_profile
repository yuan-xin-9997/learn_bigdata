# .bash_profile

# Get the aliases and functions
if [ -f ~/.bashrc ]; then
	. ~/.bashrc
fi

# User specific environment and startup programs

PATH=$PATH:$HOME/.local/bin:$HOME/bin

export PATH

########################bigdata console###############################
SHELLPATH=$HOME/shell
PATH=.:$PATH:$HOME/bin:$SHELLPATH:$SHELLPATH/console
export PATH