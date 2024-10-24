from tkinter import *
import tkinter.ttk as tk
from tkinter.scrolledtext import ScrolledText
from tkinter import filedialog
import sys

class PrintLogger(object):
    def __init__(self, textbox):
        self.textbox = textbox
    
    def write(self,text):
        pass      

class MainGUI(Tk):
    def __init__(self):
        Tk.__init__(self)
        self.title("Preservica Mass Modification Tool")
        self.columnconfigure([0,1], weight = 1, minsize=45)
        self.rowconfigure([0,1,2,4],weight = 1, minsize=45)        

    def input_section (self):
        inputf = tk.Frame(master=self,relief=RAISED,borderwidth=1)
        inputf.grid(row=0,column=0)
        inputl = tk.Label(master=inputf, text="Select Input Source")
        inputvar = StringVar(master=inputf)
        inpute = tk.Entry(master=inputf,textvariable=inputvar)
        inputb = tk.Button(master=inputf, text="Browse", command=lambda: self.browse_button_file(inputvar))
        inputl.pack(side='top')
        inpute.pack(side='left')
        inputb.pack(side='right')

    def mdir_slection(self):
        mdirf = tk.Frame(master=self,relief=RAISED,borderwidth=1)
        mdirf.grid(row=1,column=0)
        mdirl = tk.Label(master=mdirf, text="Select Metadata Source")
        mdirvar = StringVar(master=mdirf)
        mdire = tk.Entry(master=mdirf,textvariable=mdirvar)
        mdirb = tk.Button(master=mdirf, text="Browse", command=lambda: self.browse_button_dir(mdirvar))
        mdirl.pack(side='top')
        mdire.pack(side='left')
        mdirb.pack(side='right')

    def login_selection(self):
        loginf = tk.Frame(master=self,relief=RAISED,borderwidth=1)
        loginf.grid(row=2,column=0)
        userl = tk.Label(master=loginf, text="Username")
        usere = tk.Entry(master=loginf)
        passl = tk.Label(master=loginf, text="Password")
        passe = tk.Entry(master=loginf)
        serverl = tk.Label(master=loginf, text="Server")
        servere = tk.Entry(master=loginf)
        userl.pack()
        usere.pack()
        passl.pack()
        passe.pack()

    def browse_button_file(self, var):
        filed = filedialog.askopenfile(mode='r',filetypes=[('Excel Files','*.xlsx'),('CSV Files','*.csv')])
        var.set(filed)

    def browse_button_dir(self, var: StringVar):
        folderd = filedialog.askdirectory()
        var.set(folderd)

    def main(self):
        self.input_section()
        self.mdir_slection()
        self.login_selection()
        self.mainloop()

MainGUI().main()