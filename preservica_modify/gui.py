from tkinter import *
import tkinter.ttk as tk
from tkinter.scrolledtext import ScrolledText
from tkinter import filedialog
import sys
from pres_modify import PreservicaMassMod

class PrintLogger(object):
    def __init__(self, textbox):
        self.textbox = textbox
    
    def write(self,text):
        pass      

class MainGUI(Tk):
    def __init__(self):
        Tk.__init__(self)
        self.geometry( "400x600" )
        self.title("Preservica Mass Modification Tool")
        self.columnconfigure([0,1], weight = 1, minsize=20)
        self.rowconfigure([0,1,2,4,5],weight = 1, minsize=20)        

    def input_section (self):
        inputf = tk.Frame(master=self,relief=RAISED,borderwidth=1)
        inputf.grid(row=0,column=0)
        inputl = tk.Label(master=inputf, text="Select Input Source")
        self.inputvar = StringVar(master=inputf)
        inpute = tk.Entry(master=inputf,textvariable=self.inputvar)
        inputb = tk.Button(master=inputf, text="Browse", command=lambda: self.browse_button_file(self.inputvar))
        inputl.pack(side='top')
        inpute.pack(side='left')
        inputb.pack(side='right')

    def mdir_slection(self):
        mdirf = tk.Frame(master=self,relief=RAISED,borderwidth=1)
        mdirf.grid(row=1,column=0)
        mdirl = tk.Label(master=mdirf, text="Select Metadata Source")
        self.mdirvar = StringVar(master=mdirf)
        mdire = tk.Entry(master=mdirf,textvariable=self.mdirvar)
        mdirb = tk.Button(master=mdirf, text="Browse", command=lambda: self.browse_button_dir(self.mdirvar))
        mdirl.pack(side='top')
        mdire.pack(side='left')
        mdirb.pack(side='right')

    def login_selection(self):
        loginf = tk.Frame(master=self,relief=RAISED,borderwidth=1)
        loginf.grid(row=2,column=0)
        userf = tk.Frame(master=loginf, borderwidth=1)
        serverf = tk.Frame(master=loginf, borderwidth=1)
        userf.pack()
        userl = tk.Label(master=userf, text="Username")
        self.usere = tk.Entry(master=userf)
        passl = tk.Label(master=userf, text="Password")
        self.passe = tk.Entry(master=userf,show="*")
        userl.grid(row=0,column=0)
        self.usere.grid(row=1,column=0)
        passl.grid(row=0,column=1)
        self.passe.grid(row=1,column=1)

        serverf.pack(side=BOTTOM)
        serverl = tk.Label(master=serverf, text="Server")
        self.servere = tk.Entry(master=serverf)
        serverl.grid(row=0,column=0)
        self.servere.grid(row=1,column=0)

    def xml_selection(self):
        xmlf = tk.Frame(master=self, relief=RAISED,borderwidth=1)
        xmlf.grid(row=0,column=1)
        xmll = tk.Label(master=xmlf, borderwidth=1, text= "XML Method")
        self.xmlvar = StringVar()
        self.xmlvar.set('flat')
        options = ['flat', 'exact']
        xmle = tk.OptionMenu(xmlf,self.xmlvar,*options)
        xmll.pack()
        xmle.pack()
    
    def toggle_selections(self):
        togglef = tk.Frame(master=self,relief=RAISED,borderwidth=1)
        togglef.grid(row=1,column=1,rowspan=2)
        self.dummyvar = BooleanVar()
        self.dummyvar.set(False)
        self.blankvar = BooleanVar()
        self.blankvar.set(False)
        dummyb = tk.Checkbutton(master=togglef,text="Dummy Run",variable=self.dummyvar)
        blankb = tk.Checkbutton(master=togglef,text="Set Blank Overrides",variable=self.blankvar)
        dummyb.pack()
        blankb.pack()        

    def browse_button_file(self, var: StringVar):
        filed = filedialog.askopenfile(mode='r',filetypes=[('Excel Files','*.xlsx'),('CSV Files','*.csv')])
        var.set(filed.name)

    def browse_button_dir(self, var: StringVar):
        folderd = filedialog.askdirectory()
        var.set(folderd)

    def print_vars(self):
        print(self.inputvar.get(),self.mdirvar.get(),self.usere.get(),self.passe.get(),self.servere.get(),self.dummyvar.get(),self.blankvar.get())

    def run_program(self):
        runf = tk.Frame(master=self,relief=RAISED,borderwidth=1)
        runf.grid(row=5,column=0,columnspan=2)
        runb = tk.Button(master=runf,text="Run",command=self.run_command)
        runb.pack()

    def run_command(self):
        PreservicaMassMod(excel_file=self.inputvar.get(),
                          metadata_dir=self.mdirvar.get(),
                          blank_override=self.blankvar.get(),
                          dummy=self.dummyvar.get(),
                          username=self.usere.get(),
                          password=self.passe.get(),
                          server=self.servere.get())

    def main(self):
        self.input_section()
        self.mdir_slection()
        self.login_selection()
        self.xml_selection()
        self.toggle_selections()
        self.run_program()
        self.mainloop()

MainGUI().main()